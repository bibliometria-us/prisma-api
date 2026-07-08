import datetime

import pandas as pd

from config import local_config
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from routes.carga import consultas_cargas
from routes.carga.publicacion.openalex.carga import ExtraccionPublicacionOpenalex
from routes.carga.publicacion.scopus.carga import ExtraccionPublicacionScopus
from routes.carga.publicacion.wos.carga import ExtraccionPublicacionWos
from routes.carga.registro_cambios import RegistroCambios


class CargaPublicacionesBloque:
    def __init__(self, agno_inicio: int = None, agno_fin: int = None):
        bd_temp = BaseDatos()
        bd_temp.reset_auto_increment(table_name="p_publicacion")
        self.bd = BaseDatos(autocommit=False, keep_connection_alive=True)
        self.bd.startConnection()
        self.bd.start_transaction()
        self.id_carga = RegistroCambios.generar_id_carga()
        self.agno_inicio = agno_inicio or 1900
        self.agno_fin = agno_fin or datetime.datetime.now().year
        self.errores = []

    def carga_publicaciones_investigador(
        self, id_investigador: int, email: str, dry_run=False
    ):
        self.email = email
        self.autor = email.split("@")[0]
        self.id_investigador = id_investigador
        identificadores = consultas_cargas.get_id_investigadores(
            id_investigador=id_investigador
        )

        # TODO: REVISAR ORDEN EJECUCIÓN
        if identificadores.get("scopus"):
            carga_scopus = ExtraccionPublicacionScopus(
                tipo_carga="bloque",
                id_carga=self.id_carga,
                db=self.bd,
                autor=self.autor,
            )
            carga_scopus.cargar_publicaciones_por_investigador(
                identificador_origen=identificadores.get("scopus"),
                agno_inicio=self.agno_inicio,
                agno_fin=self.agno_fin,
                id_investigador=self.id_investigador,
            )
            self.errores += carga_scopus.carga.errores

        if identificadores.get("researcherid"):
            carga_wos = ExtraccionPublicacionWos(
                tipo_carga="bloque",
                id_carga=self.id_carga,
                db=self.bd,
                autor=self.autor,
            )
            carga_wos.cargar_publicaciones_por_investigador(
                identificador_origen=identificadores.get("researcherid"),
                agno_inicio=self.agno_inicio,
                agno_fin=self.agno_fin,
                id_investigador=self.id_investigador,
            )
            self.errores += carga_wos.carga.errores

        if identificadores.get("openalex"):
            return  # TODO: Desactivado temporalmente para evitar problemas de carga masiva de OpenAlex. En algún momento habría que hacer configurables los orígenes de datos.
            carga_openalex = ExtraccionPublicacionOpenalex(
                tipo_carga="bloque",
                id_carga=self.id_carga,
                db=self.bd,
                autor=self.autor,
            )
            carga_openalex.cargar_publicaciones_por_investigador(
                identificador_origen=identificadores.get("openalex"),
                agno_inicio=self.agno_inicio,
                agno_fin=self.agno_fin,
                id_investigador=self.id_investigador,
            )
            self.errores += carga_openalex.carga.errores

        self.generar_informe()

        if not dry_run:
            self.bd.commit()

        if dry_run:
            self.bd.rollback()

    def estadisticas_nuevas_publicaciones(self) -> pd.DataFrame:
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/publicacion/', pp.idPublicacion) AS 'URL',
        CASE WHEN arcp_nuevo.id IS NOT NULL THEN 'Nueva' ELSE 'Actualizada' END AS 'Estado',
        CASE WHEN COUNT(CASE WHEN pa.idInvestigador != 0 THEN 1 END) > 0 THEN "Sí" ELSE "No" END AS "Enlazado al menos un autor",
        CASE WHEN COUNT(CASE WHEN pa.idInvestigador = %(id_investigador)s THEN 1 END) > 0 THEN "Sí" ELSE "No" END AS "Enlazado este autor",
        GROUP_CONCAT(CASE WHEN pa.idInvestigador = %(id_investigador)s THEN pa.firma ELSE NULL END) AS "Firma de este autor",
        CAST(CONCAT(COUNT(CASE WHEN pa.idInvestigador != 0 THEN 1 END), "/", COUNT(pa.idInvestigador)) AS CHAR CHARACTER SET utf8mb4) AS "Enlazados/Autores totales",
        GROUP_CONCAT(CASE WHEN ii.idInvestigador != 0 THEN CONCAT(pa.orden, ". ", ii.apellidos, ", ", ii.nombre, " (", ii.idInvestigador, ")") END SEPARATOR "; ") AS "Autores US",
        id_wos.valor AS 'WOS',
        id_scopus.valor AS 'Scopus',
        id_openalex.valor AS 'Openalex',
        CASE WHEN mj.id_jcr THEN 'Sí' ELSE 'No' END AS 'JCR',
        CASE WHEN ms.id_sjr  THEN 'Sí' ELSE 'No' END AS 'SJR',
        pp.agno AS "Año",
        pp.titulo as "Título"
        FROM p_publicacion pp
        LEFT JOIN (SELECT * FROM a_registro_cambios_publicacion WHERE tipo_dato = 'id_publicacion' AND id_carga = %(id_carga)s GROUP BY id) as arcp_nuevo ON pp.idPublicacion = arcp_nuevo.id
        LEFT JOIN (SELECT * FROM a_registro_cambios_publicacion WHERE tipo_dato != 'id_publicacion' AND id_carga = %(id_carga)s GROUP BY id) as arcp_antiguo ON pp.idPublicacion = arcp_antiguo.id
        LEFT JOIN p_autor pa ON pa.idPublicacion = pp.idPublicacion
        LEFT JOIN i_investigador ii ON ii.idInvestigador = pa.idInvestigador
        LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = 'wos' GROUP BY idPublicacion) as id_wos ON id_wos.idPublicacion = pp.idPublicacion
        LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = 'scopus' GROUP BY idPublicacion) as id_scopus ON id_scopus.idPublicacion = pp.idPublicacion
        LEFT JOIN (SELECT * FROM p_identificador_publicacion WHERE tipo = 'openalex' GROUP BY idPublicacion) as id_openalex ON id_openalex.idPublicacion = pp.idPublicacion
        LEFT JOIN (SELECT * FROM m_jcr GROUP BY idFuente) as mj ON mj.idFuente = pp.idFuente
        LEFT JOIN (SELECT * FROM m_sjr GROUP BY idFuente) as ms ON ms.idFuente = pp.idFuente
        WHERE (arcp_nuevo.id IS NOT NULL OR arcp_antiguo.id IS NOT NULL)
        GROUP BY pp.idPublicacion
        """

        params = {
            "id_carga": self.id_carga,
            "id_investigador": self.id_investigador,
        }
        self.bd.ejecutarConsulta(query, params=params)

        result = self.bd.get_dataframe()

        return result

    def cambios_publicaciones(self) -> pd.DataFrame:
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/publicacion/', pp.idPublicacion) AS 'URL Prisma',
        pp.titulo as 'Título',
        arcp.origen as 'Origen',
        CONCAT_WS(' > ', tipo_dato, tipo_dato_2, tipo_dato_3) as 'Atributo añadido',
        arcp.valor as 'Valor'
        FROM a_registro_cambios_publicacion arcp
        LEFT JOIN p_publicacion pp ON pp.idPublicacion = arcp.id
        WHERE arcp.tipo_dato != 'id_publicacion'
        AND id_carga = %(id_carga)s
        ORDER BY pp.idPublicacion DESC
        """

        params = {"id_carga": self.id_carga}
        self.bd.ejecutarConsulta(query, params=params)

        result = self.bd.get_dataframe()

        return result

    def problemas_publicaciones(self) -> pd.DataFrame:
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/publicacion/', pp.idPublicacion) AS 'URL Prisma',
        pp.titulo as 'Título',
        CONCAT_WS(' > ', tipo_dato, tipo_dato_2, tipo_dato_3) as 'Atributo',
        arpp.origen_antiguo as 'Origen del valor en Prisma',
        arpp.valor_antiguo as 'Valor en Prisma',
        arpp.origen as 'Origen',
        arpp.valor as 'Valor conflictivo'
        FROM a_registro_problemas_publicacion arpp
        LEFT JOIN p_publicacion pp ON pp.idPublicacion = arpp.id
        WHERE arpp.tipo_dato != 'id_publicacion'
        AND id_carga = %(id_carga)s
        """

        params = {"id_carga": self.id_carga}
        self.bd.ejecutarConsulta(query, params=params)

        result = self.bd.get_dataframe()

        return result

    def generar_informe_errores(self) -> pd.DataFrame:
        return pd.DataFrame(self.errores, columns=["Errores"])

    def generar_headers_informe(self) -> list[list[str]]:
        query = """
        SELECT CONCAT(apellidos, ", ", nombre) AS "nombre"
        FROM i_investigador
        WHERE idInvestigador = %(id_investigador)s
        """

        params = {"id_investigador": self.id_investigador}

        self.bd.ejecutarConsulta(query, params=params)

        df = self.bd.get_dataframe()

        return [
            ["Nombre", df["nombre"].iloc[0]],
            [
                "Prisma",
                f"{local_config.prisma_url}/investigador/{self.id_investigador}",
            ],
            ["Años", f"{self.agno_inicio}-{self.agno_fin}"],
            ["Fecha de los datos", datetime.datetime.now().strftime("%d/%m/%y %H:%M")],
            [],
            [],
        ]

    def generar_informe(self):
        nuevas_publicaciones = self.estadisticas_nuevas_publicaciones()
        cambios_publicaciones = self.cambios_publicaciones()
        problemas_publicaciones = self.problemas_publicaciones()
        errores = self.generar_informe_errores()

        datos_informes: dict[str, pd.DataFrame] = {
            "Publicaciones": nuevas_publicaciones,
            "Cambios": cambios_publicaciones,
            "Problemas": problemas_publicaciones,
        }
        if not errores.empty:
            datos_informes["Errores"] = errores

        headers = self.generar_headers_informe()

        file_path = f"/tmp/nuevas_publicaciones_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            for sheet_name, df in datos_informes.items():
                # 1. Write the header rows manually using the xlsxwriter worksheet object
                # (This avoids converting the headers into a separate DataFrame)
                workbook = writer.book
                worksheet = workbook.add_worksheet(sheet_name)
                writer.sheets[sheet_name] = worksheet

                for row_idx, row_data in enumerate(headers):
                    for col_idx, value in enumerate(row_data):
                        worksheet.write(row_idx, col_idx, value)

                # 2. Write the DataFrame starting right after the headers
                # len(headers) gives us the next available row index
                df.to_excel(
                    writer, sheet_name=sheet_name, index=False, startrow=len(headers)
                )

        mensaje = f"""
                Carga de publicaciones por investigador finalizada. Se adjunta un fichero con los resultados.
                """
        if not errores.empty:
            mensaje += """
            <br> Se han detectado errores inesperados que han causado que la carga falle completa o parcialmente. El resultado de la carga se ha enviado automaticamente a la Unidad de Bibliometria para su revisión.
            """

        enviar_correo(
            adjuntos=[file_path],
            asunto="Carga de publicaciones de investigador",
            destinatarios=[self.email],
            texto_plano="",
            texto_html=mensaje,
        )

        if not errores.empty:
            enviar_correo(
                adjuntos=[file_path],
                asunto="Error en carga de publicaciones de investigador",
                destinatarios=local_config.email_admin + local_config.email_bib,
                texto_plano="",
                texto_html=mensaje,
            )
