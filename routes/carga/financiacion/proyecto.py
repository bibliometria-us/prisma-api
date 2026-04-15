from datetime import date
import random
from pandas import DataFrame
import pandas as pd
from db.conexion import BaseDatos
from utils.date import str_to_date
import config.local_config as local_config

map_tipos = {
    "Proyectos": "Proyecto",
    "Contratos Arts. 68/83": "Contrato",
    "Ayudas": "Ayuda",
    "Actividades de Transferencia": "Transferencia",
    "Redes": "Red",
    "Cátedras Específicas": "Cátedra",
    "Infraestructura Institucional": "Infraestructura",
    "Infraestructura": "Infraestructura",
}

map_ambitos = {
    "Nacional": "Nacional",
    "Europeo (EU)": "Europeo",
    "Autonómico": "Autonómico",
    "No": "Desconocido",
    "Internacional": "Internacional",
    "Local/Provincial": "Local",
    "No determinado": "Desconocido",
}


class Proyecto:
    def __init__(
        self,
        tipo: str,
        nombre: str,
        referencia: str,
        organica: str,
        inicio: date,
        fin: date,
        ambito: str,
        concedido: float,
        solicitado: float,
        prog_financiador: str,
        entidad_financiadora: str,
        competitivo: bool,
        sisius_id: int,
        visible: bool = True,
        id: int = 0,
        bd: BaseDatos = None,
    ):
        self.id = int(id)
        self.tipo = tipo
        self.nombre = nombre
        self.referencia = referencia
        self.organica = organica or None
        self.inicio = inicio
        self.fin = fin
        self.ambito = ambito
        self.concedido = concedido
        self.solicitado = solicitado or None
        self.prog_financiador = prog_financiador
        self.entidad_financiadora = entidad_financiadora
        self.competitivo = bool(competitivo)
        self.sisius_id = sisius_id
        self.visible = bool(visible)
        self.bd = bd or BaseDatos()

    def from_sisius_id(self, sisius_id: int) -> "Proyecto":

        query = """SELECT * FROM prisma_proyectos.proyecto WHERE sisius_id = %(sisius_id)s"""

        params = {
            "sisius_id": sisius_id,
        }

        self.bd.ejecutarConsulta(query, params=params)

        if not self.bd.has_rows():
            return None

        data = self.bd.get_dataframe().iloc[0]
        result = Proyecto(
            id=data["id"],
            tipo=data["tipo"],
            nombre=data["nombre"],
            referencia=data["referencia"],
            organica=data["organica"],
            inicio=data["inicio"],
            fin=data["fin"],
            ambito=data["ambito"],
            concedido=data["concedido"],
            solicitado=data["solicitado"],
            prog_financiador=data["prog_financiador"],
            entidad_financiadora=data["entidad_financiadora"],
            competitivo=data["competitivo"],
            sisius_id=data["sisius_id"],
            visible=data["visible"],
        )

        return result

    def insertar_proyecto(self) -> str:
        query = """
        INSERT INTO prisma_proyectos.proyecto (id, tipo, nombre, referencia, organica, inicio, fin, 
        ambito, concedido, solicitado, prog_financiador, entidad_financiadora, competitivo, visible, sisius_id)
        VALUES (%(id)s, %(tipo)s, %(nombre)s, %(referencia)s, %(organica)s, 
        %(inicio)s, %(fin)s, %(ambito)s, %(concedido)s, %(solicitado)s, 
        %(prog_financiador)s, %(entidad_financiadora)s, %(competitivo)s, %(visible)s, %(sisius_id)s)
        """
        params = {
            "id": self.id,
            "tipo": self.tipo,
            "nombre": self.nombre,
            "referencia": self.referencia,
            "organica": self.organica,
            "inicio": self.inicio,
            "fin": self.fin,
            "ambito": self.ambito,
            "concedido": self.concedido,
            "solicitado": self.solicitado,
            "prog_financiador": self.prog_financiador,
            "entidad_financiadora": self.entidad_financiadora,
            "competitivo": self.competitivo,
            "visible": self.visible,
            "sisius_id": self.sisius_id,
        }

        self.bd.ejecutarConsulta(query, params=params)
        self.id = self.bd.last_id

        if self.bd.error:
            return f"Error insertando el proyecto '{self.nombre}'"

        if self.bd.rowcount == 1:
            return f"Insertado el proyecto '{self.nombre}': {local_config.prisma_url}/financiacion/{self.id} "

    def actualizar_proyecto(self, proyecto_antiguo: "Proyecto") -> list[str]:
        atributos_antiguos = vars(proyecto_antiguo)
        atributos_nuevos = vars(self)

        result: list[str] = []

        for nombre, valor_nuevo in atributos_nuevos.items():
            if isinstance(valor_nuevo, BaseDatos):
                continue
            valor_antiguo = atributos_antiguos.get(nombre)
            if valor_antiguo != valor_nuevo:
                result.append(
                    self.actualizar_atributo(nombre, valor_antiguo, valor_nuevo)
                )

        return result

    def actualizar_atributo(self, nombre, valor_antiguo, valor_nuevo) -> str:
        query = f"""UPDATE prisma_proyectos.proyecto
                    SET {nombre} = %(valor_nuevo)s
                    WHERE id = %(id)s"""
        params = {
            "valor_nuevo": valor_nuevo,
            "id": self.id,
        }

        self.bd.ejecutarConsulta(query, params=params)

        if self.bd.error:
            return f"Error actualizando el atributo {nombre} con valor {str(valor_nuevo)} para el proyecto con ID {self.id}"

        return f"Actualizado atributo {nombre} de {str(valor_antiguo)} a {str(valor_nuevo)} para el proyecto con ID {self.id}"


def filtrar_proyectos(projects: DataFrame) -> DataFrame:

    # Reemplazar celdas vacías por valores nulos
    projects = projects.replace({"": None})

    # Convertir columnas de fecha a tipo fecha
    columnas_fecha = ["FechaInicio", "FechaFin"]
    for columna_fecha in columnas_fecha:
        projects[columna_fecha] = pd.to_datetime(
            projects[columna_fecha], format="%d/%m/%Y", errors="coerce"
        ).dt.date

        projects[columna_fecha] = projects[columna_fecha].replace({pd.NaT: None})

    # Descartar proyectos con fecha de inicio no existente o de año menor al 2000
    projects = projects[
        (projects["FechaInicio"] != "")
        & (
            pd.to_datetime(
                projects["FechaInicio"], format="%d/%m/%Y", errors="coerce"
            ).dt.year
            >= 2000
        )
    ]

    # Descartar proyectos de tipologías que no estén en map_tipos
    projects = projects[(projects["Modalidad"].isin(map_tipos.keys()))]

    # Normalizar nombres de ámbitos
    projects["Ambito"] = projects["Ambito"].str.replace(r"\d+", "", regex=True)
    projects["Ambito"] = projects["Ambito"].str.replace("-", "")

    # Descartar proyectos con ámbito que no esté en map_ambitos
    projects = projects[(projects["Ambito"].isin(map_ambitos.keys()))]

    # Recortar títulos con longitud mayor a 600
    titulos_largos = projects["Titulo"].str.len() > 600
    projects.loc[titulos_largos, "Titulo"] = (
        projects.loc[titulos_largos, "Titulo"].str[:595] + "[...]"
    )

    # Elimnar saltos de línea de los títulos
    projects["Titulo"] = projects["Titulo"].str.replace("\n", "", regex=True)

    return projects


def buscar_proyectos_no_en_sisius(
    bd: BaseDatos, referencias_sisius: list[str]
) -> list[str]:
    # Obtener los proyectos de Prisma que no están en SISIUS para ocultarlos
    query_proyectos_prisma = (
        """SELECT referencia, id FROM prisma_proyectos.proyecto WHERE visible = 1"""
    )
    bd.ejecutarConsulta(query_proyectos_prisma)

    proyectos_prisma = bd.get_dataframe()

    ids_prisma_no_en_sisius = proyectos_prisma[
        ~proyectos_prisma["referencia"].isin(referencias_sisius)
    ]["id"].tolist()

    return ids_prisma_no_en_sisius


def ocultar_proyectos_no_en_sisius(
    bd: BaseDatos, referencias_sisius: list[str]
) -> list[str]:
    # Ocultar los proyectos de Prisma que no están en SISIUS
    proyectos_no_en_sisius = buscar_proyectos_no_en_sisius(
        bd=bd, referencias_sisius=referencias_sisius
    )

    query_ocultar_proyectos = f"""
    UPDATE prisma_proyectos.proyecto
    SET visible = 0
    WHERE id IN ({','.join(['%s'] * len(proyectos_no_en_sisius))})
    """

    params = proyectos_no_en_sisius

    bd.ejecutarConsulta(query_ocultar_proyectos, params=params)


def cargar_proyectos(projects: DataFrame, bd: BaseDatos) -> list[str]:
    # Obtener las referencias de los proyectos en SISIUS para ocultar los que ya no están en SISIUS
    referencias_sisius = projects["Referencia"].tolist()
    ocultar_proyectos_no_en_sisius(bd=bd, referencias_sisius=referencias_sisius)

    projects = filtrar_proyectos(projects=projects)
    result = []

    for project in projects.itertuples(index=True):
        result += cargar_proyecto(project=project, bd=bd)

    return result


def cargar_proyecto(project, bd: BaseDatos) -> list[str]:
    sisius_id = int(project.IdProyecto)

    proyecto_nuevo = Proyecto(
        tipo=map_tipos.get(project.Modalidad),
        nombre=project.Titulo,
        referencia=project.Referencia,
        organica=project.Organica,
        inicio=project.FechaInicio,
        fin=project.FechaFin,
        ambito=map_ambitos.get(project.Ambito),
        concedido=float(project.ImporteConcedido),
        solicitado=float(project.ImporteSolicitado),
        prog_financiador=project.TipoProyecto,
        entidad_financiadora=project.Financiador,
        competitivo=bool(project.EsCompetitivo != "No"),
        sisius_id=sisius_id,
        visible=True,
        bd=bd,
    )

    proyecto_antiguo = proyecto_nuevo.from_sisius_id(sisius_id=sisius_id)

    if proyecto_antiguo:
        proyecto_nuevo.id = proyecto_antiguo.id
        return proyecto_nuevo.actualizar_proyecto(proyecto_antiguo=proyecto_antiguo)

    else:
        return [proyecto_nuevo.insertar_proyecto()]
