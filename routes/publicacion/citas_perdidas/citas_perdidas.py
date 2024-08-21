import os
import glob
import shutil
from typing import Dict, List
from celery import shared_task
import pandas as pd
from abc import ABC, abstractmethod

from integration.apis.clarivate.wos.wos_api import WosAPI
from integration.apis.elsevier.scopus_search.scopus_search import ScopusSearch
from integration.apis.exceptions import APIRateLimit
from integration.email.email import enviar_correo
from logger.async_request import AsyncRequest
from utils.date import get_current_date


@shared_task(
    queue="informes", name="citas_perdidas", ignore_result=True, acks_late=True
)
def cargar_citas_perdidas(request_id: str):
    request = AsyncRequest(id=request_id)
    destinatarios = request.email.split(",")
    base_dir = f"temp/citas_perdidas/{request.id}/"
    scopus_dir = f"{base_dir}scopus/"
    wos_dir = f"{base_dir}wos/"

    try:

        scopus_dataframes = [pd.DataFrame()]
        for filename in os.listdir(scopus_dir):
            df = pd.read_csv(scopus_dir + filename)
            scopus_dataframes.append(df)

        wos_dataframes = [pd.DataFrame()]
        for filename in os.listdir(wos_dir):
            df = pd.read_excel(wos_dir + filename, engine="xlrd")
            wos_dataframes.append(df)

        bd_wos = BaseDatosCitasPerdidasWoS(
            df_fuente=pd.concat(wos_dataframes),
            id=request.params["id_wos"],
        )
        bd_scopus = BaseDatosCitasPerdidasScopus(
            df_fuente=pd.concat(scopus_dataframes),
            id=request.params["id_scopus"],
        )

        citas_perdidas = CitasPerdidas(
            bds=[bd_wos, bd_scopus], id_publicacion=request.params["id_publicacion"]
        )
        citas_perdidas.comparar()

        citas_perdidas.generar_excel(base_dir=base_dir)
        citas_perdidas.generar_plantillas(base_dir=base_dir)

        request.params["citas_reclamadas_wos"] = citas_perdidas.bds[
            "WoS"
        ].citas_reclamables

        request.params["citas_reclamadas_scopus"] = citas_perdidas.bds[
            "Scopus"
        ].citas_reclamables

        enviar_correo(
            adjuntos=[
                os.path.abspath(f) for f in glob.glob(os.path.join(base_dir, "*.xlsx"))
            ],
            asunto="Informe de citas perdidas",
            destinatarios=destinatarios,
            texto_plano="",
            texto_html=citas_perdidas.resumen_mail(),
        )

        request.close(message="Informe entregado")

    except APIRateLimit:
        enviar_correo(
            adjuntos=[],
            asunto="Error: Informe de citas perdidas",
            destinatarios=destinatarios,
            texto_plano="",
            texto_html=f"""
            <p>Error en el informe de citas perdidas de la publicación 
                <a href='https://prisma.us.es/publicacion/{request.params['id_publicacion']}' target='_blank'>
                {request.params['id_publicacion']}
                </a>
            </p>
            """,
        )

        request.params["error"] = "API Limit"
        request.result = "Error"
        request.error(message="API Limit")
    except Exception as e:
        enviar_correo(
            adjuntos=[],
            asunto="Error: Informe de citas perdidas",
            destinatarios=destinatarios,
            texto_plano="",
            texto_html=f"""
            <p>Error en el informe de citas perdidas de la publicación 
                <a href='https://prisma.us.es/publicacion/{request.params['id_publicacion']}' target='_blank'>
                {request.params['id_publicacion']}
                </a>
            </p>
            """,
        )

        request.params["error"] = str(e)
        request.error(message="Error desconocido")
    finally:
        shutil.rmtree(base_dir)

    return None


class CitasPerdidas:
    def __init__(
        self, bds: List["BaseDatosCitasPerdidas"], id_publicacion: str
    ) -> None:
        self.bds = {bd.nombre: bd for bd in bds}
        self.id_publicacion = id_publicacion

    def comparar(self):
        for nombre, bd in self.bds.items():
            # Para cada base de datos, cogemos la lista de todas las bases de datos que no sean ella misma
            bds_objetivo = [
                bd_objetivo
                for nombre_bd_objetivo, bd_objetivo in self.bds.items()
                if nombre_bd_objetivo != nombre
            ]

            for bd_objetivo in bds_objetivo:
                bd.comparar(bd_objetivo)
                bd.buscar_no_indexadas_por_doi(bd_objetivo)

    def generar_plantillas(self, base_dir: str):
        for nombre, bd in self.bds.items():
            # Para cada base de datos, cogemos la lista de todas las bases de datos que no sean ella misma
            bds_objetivo = [
                bd_objetivo
                for nombre_bd_objetivo, bd_objetivo in self.bds.items()
                if nombre_bd_objetivo != nombre
            ]

            for bd_objetivo in bds_objetivo:
                bd.generar_plantilla(bd_objetivo, base_dir)

    def generar_excel(self, base_dir: str):
        os.makedirs("temp/citas_perdidas/", exist_ok=True)
        with pd.ExcelWriter(
            base_dir + f"citas_perdidas_{self.id_publicacion}.xlsx",
            engine="xlsxwriter",
        ) as writer:
            # Iterate over all sheets in the dictionary and write each sheet to the new file
            for sheet_name, bd in self.bds.items():
                bd.df[bd.columnas_excel].to_excel(
                    writer, sheet_name=sheet_name, index=False, startrow=2
                )

                workbook = writer.book
                worksheet = writer.sheets[sheet_name]

                header = bd.header_excel()
                worksheet.write(0, 0, header)
                worksheet.merge_range(0, 0, 0, len(bd.df.columns) - 1, header)

    def resumen_mail(self):
        id_publicacion = self.id_publicacion
        result = f"""
        <p>Resultado de la búsqueda de citas perdidas para la <a href='https://prisma.us.es/publicacion/{id_publicacion}'>publicación {id_publicacion}</a> </p>
        <br>
        <p><b>Citas originales: </b></p>
        <ul>
            <li>{len(self.bds["WoS"].df)} citas en WoS</li>
            <li>{len(self.bds["Scopus"].df)} citas en Scopus</li>
        </ul>
        <p><b>Citas que han sido detectadas automáticamente para ser reclamadas: </b></p>
        <ul>
            <li>{self.bds["WoS"].citas_reclamables} citas en WoS</li>
            <li>{self.bds["Scopus"].citas_reclamables} citas en Scopus</li>
        </ul>
        <br>
        <p>Se incluyen ficheros ya rellenos para adjuntar y reclamar las citas perdidas en cada base de datos. Estos ficheros contienen las citas detectadas automáticamente, 
        si manualmente detecta más citas perdidas, añádalas al fichero siguiendo el mismo formato.</p>
        <p><b>Instrucciones para reclamar citas en WoS:</b></p>
        <ul>
            <li>
            Adjuntar plantilla de reclamación citas en WoS en un correo dirigido a: <b>ts.agdatacorrections@clarivate.com</b>
            </li>
            <li>Asunto: <b>Missing citations for WoS:xxxxx</b></li>
            <li>
                Cuerpo:<br>
                <b>Dear,<br>
                Please see attached citation template file with missing citations.<br>
                Thanks in advance.<br>
                Regards,</b>
            </li>
            <li>
            Debe conservar el correo que recibirá con el ticket asignado a su petición: Ej. Clarivate Case # CM-240620-7302037<br>
            Pasado un mes, si las citas no se han actualizado, reenviar el correo con el ticket indicando que aún está pendiente.
            </li>
        </ul>
        <p><b>Instrucciones para reclamar citas en Scopus:</b></p>
        <ul>
            <li>
                Acceder al formulario en 
                <a href="https://service.elsevier.com/app/contact/supporthub/scopuscontent/">
                https://service.elsevier.com/app/contact/supporthub/scopuscontent/
                </a>
            </li>
            <li>
                Completar el formulario:
                <ul>
                    <li>
                        <p><b>Role:</b> marcar la opción que corresponda, librarian, author, customer.</p>
                        <p><b>Contact reason:</b> Citation correction</p>
                    </li>
                        <p><b>Subject:</b></p>
                        <p>Si estamos reclamando varias citas perdidas, indicar: See attached file.</p>
                        <p>Si solo reclamamos 1 cita, debemos escribir algo similar a esto, con los datos específicos de nuestra reclamación:</p>
                        <p><b>Cited article title:</b> A generalized theory of gravitation</p>
                        <p><b>Cited article link in Scopus:</b> http://www.scopus.com/record/...</p>
                    <li>
                        <b>Your question:</b> Citation correction for EID: XXXXXXXXXXX
                    </li>
                    <li>
                        Adjuntar fichero
                    </li>
                    <li>
                        Cumplimentar los datos personales con correo institucional @us.es
                    </li>
                    <li>
                        Pulsar Send your question.
                    </li>
                    
                </ul>
            </li>
            <li>
                Se recibirá un correo con el ticket generado, que deberá conservarse para hacer el seguimiento.
            </li>

        </ul>
        """

        return result


class BaseDatosCitasPerdidas(ABC):
    def __init__(self, nombre: str, df_fuente: pd.DataFrame, id: str) -> None:
        self.nombre = nombre
        self.id = id
        self.df_fuente = df_fuente
        self.df = pd.DataFrame()
        self.comparados: list[str] = []
        self.columnas_excel = ["DOI", "Título", "ID", "Año"]
        self.citas_reclamables = 0
        self.cargar_df()

    @abstractmethod
    def get_url(self, index):
        pass

    def iniciar_df(self):
        columns = [column for column in self.map_columnas] + ["URL"]
        self.df = pd.DataFrame(columns=columns)

    def cargar_df(self):
        self.iniciar_df()

        for index, row in self.df_fuente.iterrows():
            new_row = {key: row[value] for key, value in self.map_columnas.items()}
            new_row["URL"] = self.get_url(index)

            new_df = pd.DataFrame(new_row, index=[index])

            self.df = pd.concat([self.df, new_df])
            self.df.drop_duplicates()

        return None

    def comparar(self, target_db: "BaseDatosCitasPerdidas"):
        self.comparados.append(target_db.nombre)
        columna_comparacion = "Cita indexada en " + target_db.nombre
        self.df[columna_comparacion] = pd.Series(dtype="object")
        self.columnas_excel.append(columna_comparacion)

        for index, row in self.df.iterrows():
            doi = row["DOI"]

            doi_encontrado = doi in target_db.df["DOI"].values

            if doi_encontrado:
                self.df.at[index, columna_comparacion] = "Sí"
            else:
                self.df.at[index, columna_comparacion] = "No"

        return None

    def buscar_no_indexadas_por_doi(self, target_db: "BaseDatosCitasPerdidas"):
        columna_cita_indexada_en = "Cita indexada en " + target_db.nombre

        dois = self.df.loc[
            (self.df[columna_cita_indexada_en] == "No") & (pd.notna(self.df["DOI"])),
            "DOI",
        ].to_list()

        busqueda_dois_indexados = target_db.buscar_dois_en_api(dois)

        columna_documento_indexado_en = "Documento indexado en " + target_db.nombre
        columna_url_documento_indexado = "URL en " + target_db.nombre
        self.df[columna_documento_indexado_en] = pd.Series(dtype="object")
        self.columnas_excel.append(columna_documento_indexado_en)
        self.columnas_excel.append(columna_url_documento_indexado)

        # Etiquetar las citas que no están indexadas pero que la publicación sí ha sido encontrada
        self.df = pd.merge(
            self.df,
            busqueda_dois_indexados[pd.notnull(busqueda_dois_indexados["DOI"])],
            on=["DOI"],
            how="left",
            suffixes=("", f" en {target_db.nombre}"),
        )
        self.df.loc[
            ~(pd.isnull(self.df[columna_url_documento_indexado])),
            columna_documento_indexado_en,
        ] = "Sí"

        self.df.loc[
            (pd.isnull(self.df[columna_url_documento_indexado]))
            & (self.df[columna_cita_indexada_en] == "No"),
            columna_documento_indexado_en,
        ] = "No"

        return None

    def generar_plantilla(self, target_db: "BaseDatosCitasPerdidas", base_dir: str):
        columna_id_articulo_citante = self.id

        plantilla = target_db.df.loc[
            target_db.df[f"Documento indexado en {self.nombre}"] == "Sí",
            [
                f"Título en {self.nombre}",
                f"ID en {self.nombre}",
                f"Año en {self.nombre}",
            ],
        ].rename(
            columns={
                f"Título en {self.nombre}": "Citing Article--Title",
                f"ID en {self.nombre}": "Citing Article--Accession Number",
                f"Año en {self.nombre}": "Citing Article--Year Published",
            }
        )

        indices = pd.Series(
            range(1, len(plantilla) + 1), name="Number", index=plantilla.index
        )
        cited_article_id = pd.Series(
            [columna_id_articulo_citante] * len(plantilla),
            name="Cited Article--Accession Number",
            index=plantilla.index,
        )

        plantilla = pd.concat([indices, cited_article_id, plantilla], axis=1)
        self.citas_reclamables = len(plantilla)

        filename = f"reclamacion_{self.nombre.lower()}.xlsx"

        with pd.ExcelWriter(
            base_dir + filename,
            engine="xlsxwriter",
        ) as writer:
            plantilla.to_excel(writer, index=False)

    @abstractmethod
    def buscar_dois_en_api(self, dois) -> pd.DataFrame:
        pass

    def header_excel(self):
        return f"Citas de la publicación con identificador {self.id} en {self.nombre}"


class BaseDatosCitasPerdidasWoS(BaseDatosCitasPerdidas):
    def __init__(self, df_fuente: pd.DataFrame, id: str) -> None:
        self.map_columnas = {
            "DOI": "DOI",
            "Título": "Article Title",
            "ID": "UT (Unique WOS ID)",
            "Año": "Publication Year",
        }
        self.api = WosAPI()
        self.url_template = "https://www.webofscience.com/wos/woscc/full-record/{id}"
        super().__init__(nombre="WoS", df_fuente=df_fuente, id=id)

    def get_url(self, index):
        id = self.df_fuente.iloc[index][self.map_columnas["ID"]]
        return self.url_template.format(id=id)

    def buscar_dois_en_api(self, dois: List[Dict]) -> pd.DataFrame:
        record_data = pd.DataFrame(columns=["DOI", "URL", "ID", "Título", "Año"])

        if not dois:
            return record_data

        self.api.search_by_DOI_list(dois)
        records: List[Dict] = self.api.records

        for record in records:
            try:
                data = {}
                identifiers: List[Dict] = record["dynamic_data"]["cluster_related"][
                    "identifiers"
                ]["identifier"]
                doi = [
                    identifier["value"]
                    for identifier in identifiers
                    if identifier["type"] in ["doi", "xref_doi"]
                ][0]
                uid = record["UID"]

                data["DOI"] = doi
                data["URL"] = self.url_template.format(id=uid)
                data["ID"] = uid
                titles: List[Dict] = record["static_data"]["summary"]["titles"]["title"]
                data["Título"] = [
                    title["content"] for title in titles if title["type"] == "item"
                ][0]
                data["Año"] = str(
                    record["static_data"]["summary"]["pub_info"]["pubyear"]
                )

                record_data.loc[len(record_data)] = data

            except:
                pass

        return record_data


class BaseDatosCitasPerdidasScopus(BaseDatosCitasPerdidas):
    def __init__(self, df_fuente: pd.DataFrame, id: str) -> None:
        self.map_columnas = {
            "DOI": "DOI",
            "Título": "Title",
            "ID": "EID",
            "Año": "Year",
        }
        self.api = ScopusSearch()
        self.url_template = (
            "https://www.scopus.com/record/display.uri?eid={id}&origin=resultslist"
        )

        super().__init__(nombre="Scopus", df_fuente=df_fuente, id=id)

    def get_url(self, index):
        eid = self.df_fuente.iloc[index][self.map_columnas["ID"]]
        return self.url_template.format(id=eid)

    def buscar_dois_en_api(self, dois: List[str]) -> pd.DataFrame:
        data_list = pd.DataFrame(columns=["DOI", "URL", "ID", "Título", "Año"])

        if not dois:
            return data_list

        self.api.search_by_DOI_list(dois)

        api_results: List[dict] = self.api.results

        for result in api_results:
            data = {}

            data["DOI"] = result.get("prism:doi", None)
            eid = result.get("eid", None)
            data["URL"] = self.url_template.format(id=eid)
            data["ID"] = eid
            data["Título"] = result.get("dc:title")
            data["Año"] = result.get("prism:coverDisplayDate")

            data_list.loc[len(data_list)] = data

        return data_list


def buscar_citas_perdidas(citas_wos: pd.DataFrame, citas_scopus: pd.DataFrame) -> None:
    bd_wos = BaseDatosCitasPerdidasWoS(df_fuente=citas_wos)
    bd_scopus = BaseDatosCitasPerdidasScopus(df_fuente=citas_scopus)

    citas_perdidas = CitasPerdidas(bds=[bd_wos, bd_scopus])
    citas_perdidas.comparar()
    citas_perdidas.generar_excel()
    pass
