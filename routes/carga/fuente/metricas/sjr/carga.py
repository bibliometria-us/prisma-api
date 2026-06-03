import time
from pandas import DataFrame

from db.conexion import BaseDatos
from routes.carga.carga import Carga
from routes.carga.fuente.metricas.sjr.datos import DatosCargaSJR
from routes.carga.fuente.metricas.sjr.extraccion import ExtraccionSJR
from routes.carga.fuente.metricas.sjr.registros_cambio_sjr import RegistroCambiosSJR


class CargaSJR(Carga):
    def __init__(
        self,
        year,
        db=None,
        id_carga=None,
        auto_commit=True,
        autor=None,
        tipo_carga=None,
    ):
        super().__init__(db, id_carga, auto_commit, autor, tipo_carga)
        self.year = year

    def carga_sjr(self, data: DataFrame, dry_run=False):
        extraccion = ExtraccionSJR()
        extraccion.leer_datos(data, self.year)

        for categoria in extraccion.categorias.values():
            for dato in categoria.datos:
                self.guardar_dato(dato)

    def guardar_dato(self, dato: DatosCargaSJR):
        if not dato.quartile:
            return

        dato.id_fuente = self.buscar_id_fuente(dato)

        dato_existente = self.buscar_sjr(dato)

        if dato_existente:
            self.actualizar_dato(dato=dato, dato_existente=dato_existente)
        else:
            self.guardar_nuevo_dato(dato)

        self.insertar_registros()
        self.lista_registros = []

    def actualizar_dato(self, dato: DatosCargaSJR, dato_existente: DatosCargaSJR):
        query = """
            UPDATE m_sjr
            SET idFuente = %(id_fuente)s, impact_factor = %(impact_factor)s, rank = %(rank)s, quartile = %(quartile)s, decil = %(decil)s, tercil = %(tercil)s
            WHERE journal = %(journal)s AND year = %(year)s AND category = %(category)s AND issn = %(issn)s AND issn_2 = %(issn_2)s
        """

        params = {
            "id_fuente": dato.id_fuente,
            "journal": dato.journal,
            "year": dato.year,
            "category": dato.category,
            "issn": dato.issn,
            "issn_2": dato.issn_2,
            "impact_factor": dato.impact_factor,
            "rank": dato.rank,
            "quartile": dato.quartile,
            "decil": dato.decil,
            "tercil": dato.tercil,
        }

        self.db.ejecutarConsulta(query, params)

        atributos_a_actualizar = [
            "id_fuente",
            "impact_factor",
            "rank",
            "quartile",
            "decil",
            "tercil",
        ]

        if not dato.id_fuente:
            return

        for atributo in atributos_a_actualizar:
            valor = getattr(dato, atributo)
            valor_antiguo = getattr(dato_existente, atributo)
            if valor != valor_antiguo:
                registro = RegistroCambiosSJR(
                    id=dato.id_fuente,
                    atributo=atributo,
                    valor=valor,
                    valor_antiguo=valor_antiguo,
                    year=self.year,
                    bd=self.db,
                )
                self.lista_registros.append(registro)

    def guardar_nuevo_dato(self, dato: DatosCargaSJR):
        query = """
            INSERT INTO m_sjr (idFuente, journal, issn, issn_2, year, category, impact_factor, rank, quartile, decil, tercil)
            VALUES (%(id_fuente)s, %(journal)s, %(issn)s, %(issn_2)s, %(year)s, %(category)s, %(impact_factor)s, %(rank)s, %(quartile)s, %(decil)s, %(tercil)s)
        """

        params = {
            "id_fuente": dato.id_fuente,
            "journal": dato.journal,
            "issn": dato.issn,
            "issn_2": dato.issn_2,
            "year": dato.year,
            "category": dato.category,
            "impact_factor": dato.impact_factor,
            "rank": dato.rank,
            "quartile": dato.quartile,
            "decil": dato.decil,
            "tercil": dato.tercil,
        }

        self.db.ejecutarConsulta(query, params)

        if not dato.id_fuente:
            return

        registro = RegistroCambiosSJR(
            id=dato.id_fuente,
            atributo="nuevo_sjr",
            valor=f"SJR {dato.impact_factor}: {dato.rank} ({dato.category})",
            valor_antiguo=None,
            year=self.year,
            bd=self.db,
        )

        self.lista_registros.append(registro)

    def buscar_id_fuente(self, dato: DatosCargaSJR):
        query = """
            SELECT pif.idFuente FROM p_identificador_fuente pif
            INNER JOIN p_fuente pf ON pf.idFuente = pif.idFuente
            WHERE pif.tipo IN ('issn', 'eissn') AND pif.valor IN (%(issn)s, %(issn_2)s)
            LIMIT 1
        """

        params = {"issn": dato.issn, "issn_2": dato.issn_2}

        self.db.ejecutarConsulta(query, params)

        id_fuente = self.db.get_first_cell()

        return id_fuente

    def buscar_sjr(self, dato: DatosCargaSJR) -> DatosCargaSJR:
        query = """
            SELECT * FROM m_sjr
            WHERE journal = %(journal)s AND year = %(year)s AND category = %(category)s AND issn = %(issn)s AND COALESCE(issn_2, '') = %(issn_2)s
        """
        params = {
            "journal": dato.journal,
            "year": dato.year,
            "category": dato.category,
            "issn": dato.issn,
            "issn_2": dato.issn_2 or "",
        }

        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        dato = (
            DatosCargaSJR(
                id_fuente=(
                    int(df["idFuente"].iloc[0]) if df["idFuente"].iloc[0] else None
                ),
                journal=df["journal"].iloc[0],
                issn=df["issn"].iloc[0],
                issn_2=df["issn_2"].iloc[0],
                year=df["year"].iloc[0],
                category=df["category"].iloc[0],
                impact_factor=float(df["impact_factor"].iloc[0]),
                rank=df["rank"].iloc[0],
                quartile=df["quartile"].iloc[0],
                decil=df["decil"].iloc[0],
                tercil=df["tercil"].iloc[0],
            )
            if not df.empty
            else None
        )

        return dato

    def limpiar_registros_importacion(self):
        pass

    def procesar_registros(self):
        pass
