from db.conexion import BaseDatos
from integration.apis.api import API
from urllib.parse import quote
from integration.apis.clarivate.journals import config
from integration.apis.clarivate.journals.exceptions import ErrorCargaMetrica, ISSNsNoEncontrados, MetricaNoEncontrada, MetricasNoEncontradas, RevistaWosNoEncontrada
from integration.apis.exceptions import APIRateLimit
from logger.logger import Log, TaskLogger
from utils.cuantiles import calcular_cuantil
from abc import ABC, abstractmethod

class JournalsAPI(API):
    def __init__(self, db: BaseDatos, año: int, fecha_carga:str, id_fuente: int) -> None:
        super().__init__()
        self.db = db
        self.id_fuente = id_fuente
        self.año = año
        self.id_wos = None
        self.titulo_revista = None
        self.issn = None
        self.eissn = None
        self.add_headers({
            "accept" : "application/json",
            "X-ApiKey" : config.api_key
        })
        self.response_type = "json"
        self.jif: JIF = None
        self.jci: JCI = None
        self.logger = TaskLogger(task_id=f"{id_fuente}-{año}", date=fecha_carga, task_name="carga_wos_journal")
        self.logger.metadata.parse()

    def get_respose(self) -> dict:
        result = super().get_respose()

        if result.get("message") == "API rate limit exceeded":
            raise APIRateLimit()

        return result

    def carga(self) -> None:
        self.logger.metadata.start_task()
        self.buscar_id_wos()
        self.buscar_issns()
        self.obtener_metricas()
        self.logger.close()

    def buscar_id_wos(self) -> str:
        busqueda_db = self.buscar_id_wos_db()

        if not busqueda_db:
            self.buscar_id_wos_api()
            self.almacenar_id_wos()
        
        return self.id_wos

    def buscar_id_wos_db(self) -> bool:

        query_busqueda = "SELECT valor FROM p_identificador_fuente WHERE tipo = 'wos' AND idFuente = %s"
        params_busqueda = [self.id_fuente]

        resultado_busqueda = self.db.ejecutarConsulta(query_busqueda, params_busqueda)

        encontrado = (len(resultado_busqueda) > 1)
        if encontrado:
            self.id_wos = resultado_busqueda[1][0]

        return encontrado

    def buscar_id_wos_api(self) -> None:
        self.uri_template = "https://api.clarivate.com/apis/wos-journals/v1/journals?q={query}&page=1&limit=1"
        
        query_issns = """SELECT GROUP_CONCAT(valor SEPARATOR ",") as issns FROM `p_identificador_fuente`
                        WHERE tipo IN ("issn","eissn") AND idFuente = %s
                        GROUP BY idFuente"""
        
        params = [self.id_fuente]

        resultado_query_issn = self.db.ejecutarConsulta(query_issns, params)

        if len(resultado_query_issn) <= 1:
            exception = ISSNsNoEncontrados(self.id_fuente)
            self.logger.add_exception_log(exception, "warning", close=True)
            raise exception

        issns = self.db.ejecutarConsulta(query_issns, params)[1][0].split(',')

        
        wos_q_issns = " OR ".join(issns)

        self.add_uri_data({"query":quote(wos_q_issns)})

        self.format_uri()

        self.get_respose()
        
        if self.response.get("metadata").get("total") == 0:  
            exception = RevistaWosNoEncontrada(self.id_fuente, issns)
            self.logger.add_exception_log(exception, "info", close=True)
            raise exception

        hits = self.response.get("hits")
        self.id_wos = str(hits[0].get("id"))
        self.titulo_revista = str(hits[0].get("name"))
        

        return None
    
    def almacenar_id_wos(self) -> None:
        query_almacenar_id = """INSERT INTO p_identificador_fuente (idFuente, tipo, valor, comentario, eliminado)
                                VALUES (%s, %s, %s, %s, %s)"""
        params = [self.id_fuente, 'wos', self.id_wos, None, 0]

        self.db.ejecutarConsulta(query_almacenar_id, params)

    def buscar_issns(self) -> None:
        self.uri_template = "https://api.clarivate.com/apis/wos-journals/v1/journals/{id}"
        self.add_uri_data({
            "id": self.id_wos
        })
        self.format_uri()
        self.get_respose()

        self.issn = self.response.get("issn") or (self.response.get("previousIssn")[0] if self.response.get("previousIssn") else None)
        self.eissn = self.response.get("eIssn")

        return None

    def obtener_metricas(self) -> None:
        
        self.uri_template = "https://api.clarivate.com/apis/wos-journals/v1/journals/{id}/reports/year/{year}"
        self.add_uri_data({
            "id": self.id_wos,
            "year": self.año,
        })
        self.format_uri()
        metricas = self.get_respose()

        if metricas.get("Error"):
            if metricas.get("Error").startswith("404"):
                exception = MetricasNoEncontradas(self.id_wos, self.id_fuente, self.año)
                self.logger.add_exception_log(exception, "info", close=True)
                raise exception
        
        if not metricas.get("ranks"):
                exception = MetricasNoEncontradas(self.id_wos, self.id_fuente, self.año)
                self.logger.add_exception_log(exception, "info", close=True)
                raise exception
        
        if not metricas["ranks"].get("jif"):
            exception = MetricaNoEncontrada(self.id_wos, self.id_fuente, self.año, "JIF")
            self.logger.add_exception_log(exception, "info")
        else:
            jif: dict = metricas["ranks"]["jif"][0]

            self.jif = JIF(
                journal=metricas["journal"]["name"],
                issn=self.issn,
                issn_2=self.eissn,
                year=metricas["year"],
                edition=jif["edition"],
                category=jif["category"],
                impact_factor=metricas["metrics"]["impactMetrics"].get("jif"),
                rank=jif.get("rank"),
                percentile=jif.get("jifPercentile"),
                id_fuente=self.id_fuente,
                db=self.db,
                id_wos = self.id_wos,           
            )

            try:
                if not self.jif.almacenar():
                    exception = MetricaNoEncontrada(self.id_wos, self.id_fuente, self.año, "JIF")
                    self.logger.add_exception_log(exception, "info")
                else:
                    log = Log(f"Actualizado JIF para la revista {self.id_fuente} el año {self.año}", "info")
                    self.logger.add_log(log)
            
            except ErrorCargaMetrica as e:
                self.logger.add_exception_log(e, "error")

        if not metricas["ranks"].get("jci"):
            exception = MetricaNoEncontrada(self.id_wos, self.id_fuente, self.año, "JCI")
            self.logger.add_exception_log(exception, "info")
    
        else:
            jci: dict = metricas["ranks"]["jci"][0]

            self.jci = JCI(
                journal=metricas["journal"]["name"],
                issn=self.issn,
                issn_2=self.eissn,
                year=metricas["year"],
                edition = None,
                category=jci["category"],
                impact_factor=metricas["metrics"]["impactMetrics"].get("jci"),
                rank=jci.get("rank"),
                percentile=jci.get("jciPercentile"),
                id_fuente=self.id_fuente,
                db=self.db,
                id_wos = self.id_wos,
            )
            try:
                if not self.jci.almacenar():
                    exception = MetricaNoEncontrada(self.id_wos, self.id_fuente, self.año, "JCI")
                    self.logger.add_exception_log(exception, "info")
                else:
                    log = Log(f"Actualizado JCI para la revista {self.id_fuente} el año {self.año}", "info")
                    self.logger.add_log(log)
            
            except ErrorCargaMetrica as e:
                self.logger.add_exception_log(e, "error")



    

    

class MetricaWoS(ABC):
    def __init__(self, journal: str, issn: str, issn_2: str, year: str, edition: str, category: str, impact_factor:float, rank:str, percentile:float, id_fuente: int, db: BaseDatos, id_wos: str) -> None:
        self.journal = journal
        self.issn = issn
        self.issn_2 = issn_2 if issn != issn_2 else None
        self.year = year
        self.edition = edition
        self.category = category
        self.impact_factor = impact_factor
        self.rank = rank
        self.percentile = percentile
        self.id_fuente = id_fuente
        self.db = db
        self.quartile = None
        self.decil = None
        self.tercil = None
        self.calcular_cuantiles()
        self.id_wos = id_wos

    def calcular_cuantiles(self):
        if self.percentile:
            self.quartile = calcular_cuantil(self.percentile, "cuartil")
            self.decil = calcular_cuantil(self.percentile, "decil")
            self.tercil = calcular_cuantil(self.percentile, "tercil")
    
    @abstractmethod
    def almacenar(self) -> bool:
        pass

class JIF(MetricaWoS):
    def almacenar(self):
        if self.rank and (self.issn or self.issn_2):
            query = """REPLACE INTO m_jcr (journal, issn, issn_2, year, edition, category, impact_factor, rank, quartile, decil, tercil, idFuente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            params = [self.journal, self.issn, self.issn_2, self.year, self.edition, self.category, self.impact_factor, self.rank,
                    self.quartile, self.decil, self.tercil, self.id_fuente]

            result = self.db.ejecutarConsulta(query, params)

            if result == [()]:
                return True
            else:
                raise ErrorCargaMetrica(self.id_wos, self.id_fuente, self.year, "JIF")
                
        return False

    
class JCI(MetricaWoS):
    def almacenar(self) -> bool:
        if self.rank and (self.issn or self.issn_2):
            query = """REPLACE INTO m_jci (revista, issn, issn_2, agno, categoria, jci, posicion, cuartil, decil, tercil, percentil, idFuente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            params = [self.journal, self.issn, self.issn_2, self.year, self.category, self.impact_factor, self.rank,
                    self.quartile, self.decil, self.tercil, self.percentile, self.id_fuente]

            result = self.db.ejecutarConsulta(query, params)

            if result == [()]:
                return True
            else:
                raise ErrorCargaMetrica(self.id_wos, self.id_fuente, self.year, "JCI")

        return False


