from datetime import datetime

from pandas import DataFrame
import pandas as pd

from config import local_config
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from routes.carga.fuente.metricas.sjr.carga import CargaSJR
from routes.carga.registro_cambios import RegistroCambios


class ImportarSJR:
    def __init__(self, year: int):
        self.year = year
        self.db = BaseDatos(keep_connection_alive=True, autocommit=False)
        self.id_carga = RegistroCambios.generar_id_carga()
        self.carga = CargaSJR(
            year=self.year,
            autor="Unidad de Bibliometría",
            id_carga=self.id_carga,
            db=self.db,
        )

    def importar(self, data: DataFrame, dry_run=False):

        self.db.startConnection()
        self.db.start_transaction()

        self.carga.carga_sjr(data=data)
        self.generar_informe()

        if not dry_run:
            self.db.commit()
        if dry_run:
            self.db.rollback()

    def generar_informe(self):
        estadisticas_nuevo_sjr = self.estadisticas_nuevo_sjr()
        estadisticas_actualizados_sjr = self.estadisticas_actualizados_sjr()

        datos_informes = {
            "nuevo": estadisticas_nuevo_sjr,
            "actualizados": estadisticas_actualizados_sjr,
        }

        file_path = f"/tmp/carga_sjr_{str(self.year)}.xlsx"
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            for sheet_name, df in datos_informes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        enviar_correo(
            adjuntos=[file_path],
            asunto="Carga de SJR",
            destinatarios=local_config.email_bib,
            texto_plano="",
            texto_html="",
        )

    def estadisticas_nuevo_sjr(self) -> DataFrame:
        query = f"""
        SELECT 
        CONCAT('{local_config.prisma_url}', '/fuente/', ms.idFuente) AS "URL",
        ms.year AS "Año",
        ms.category AS "Categoría",
        ms.impact_factor AS "SJR",
        ms.rank AS "Ranking",
        ms.quartile AS "Cuartil",
        ms.decil AS "Decil",
        ms.tercil AS "Tercil"
        FROM m_sjr ms
        INNER JOIN a_registro_cambios_fuente arcf ON arcf.id = ms.idFuente
        WHERE arcf.tipo_dato = 'sjr'
        AND arcf.tipo_dato_3 = 'nuevo_sjr'
        AND arcf.valor_antiguo IS NULL
        AND arcf.id_carga = %(id_carga)s
        AND ms.year = %(year)s
        GROUP BY ms.idFuente, ms.category, ms.year
        """

        params = {"id_carga": self.id_carga, "year": self.year}

        self.db.ejecutarConsulta(query, params)

        result = self.db.get_dataframe()
        return result

    def estadisticas_actualizados_sjr(self) -> DataFrame:
        query = f"""
        SELECT 
        CONCAT('{local_config.prisma_url}', '/fuente/', id) AS "URL",
        tipo_dato_2 AS "Año",
        tipo_dato_3 AS "Tipo de dato",
        valor_antiguo AS "Valor antiguo",
        valor AS "Valor nuevo"
        FROM a_registro_cambios_fuente
        WHERE tipo_dato = 'sjr'
        AND valor_antiguo IS NOT NULL
        AND id_carga = %(id_carga)s
        ORDER BY id ASC
        """

        params = {"id_carga": self.id_carga}

        self.db.ejecutarConsulta(query, params)

        result = self.db.get_dataframe()
        return result
