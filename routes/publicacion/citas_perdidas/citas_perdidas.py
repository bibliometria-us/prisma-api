import os
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

        scopus_dataframes = []
        for filename in os.listdir(scopus_dir):
            df = pd.read_csv(scopus_dir + filename)
            scopus_dataframes.append(df)

        wos_dataframes = []
        for filename in os.listdir(wos_dir):
            df = pd.read_excel(wos_dir + filename, engine="xlrd")
            wos_dataframes.append(df)

        bd_wos = BaseDatosCitasPerdidasWoS(df_fuente=pd.concat(wos_dataframes))
        bd_scopus = BaseDatosCitasPerdidasScopus(df_fuente=pd.concat(scopus_dataframes))

        citas_perdidas = CitasPerdidas(bds=[bd_wos, bd_scopus])
        citas_perdidas.comparar()
        citas_perdidas.generar_excel(filename=base_dir + "citas_perdidas.xlsx")

        enviar_correo(
            adjuntos=[base_dir + "citas_perdidas.xlsx"],
            asunto="Informe de citas perdidas",
            destinatarios=destinatarios,
            texto_plano="",
            texto_html=f"""
            <p>Informe de citas perdidas de la publicación 
                <a href='https://prisma.us.es/publicacion/{request.params['id_publicacion']}' target='_blank'>
                {request.params['id_publicacion']}
                </a>
            </p>
            """,
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
    def __init__(self, bds: List["BaseDatosCitasPerdidas"]) -> None:
        self.bds = {bd.nombre: bd for bd in bds}

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

    def generar_excel(self, filename: str):
        os.makedirs("temp/citas_perdidas/", exist_ok=True)
        with pd.ExcelWriter(
            filename,
            engine="xlsxwriter",
        ) as writer:
            # Iterate over all sheets in the dictionary and write each sheet to the new file
            for sheet_name, bd in self.bds.items():
                bd.df.to_excel(writer, sheet_name=sheet_name, index=False)


class BaseDatosCitasPerdidas(ABC):
    def __init__(self, nombre: str, df_fuente: pd.DataFrame) -> None:
        self.nombre = nombre
        self.df_fuente = df_fuente
        self.df = pd.DataFrame()
        self.comparados: list[str] = []
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
        self.df.loc[:, columna_comparacion] = None
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
        self.df.loc[:, columna_documento_indexado_en] = None

        # Etiquetar las citas que no están indexadas pero que la publicación sí ha sido encontrada
        self.df = pd.merge(
            self.df,
            busqueda_dois_indexados,
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

    @abstractmethod
    def buscar_dois_en_api(self, dois) -> pd.DataFrame:
        pass


class BaseDatosCitasPerdidasWoS(BaseDatosCitasPerdidas):
    def __init__(self, df_fuente: pd.DataFrame) -> None:
        self.map_columnas = {
            "DOI": "DOI",
            "Título": "Article Title",
            "ID": "UT (Unique WOS ID)",
            "Año": "Publication Year",
        }
        self.api = WosAPI()
        self.url_template = "https://www.webofscience.com/wos/woscc/full-record/{id}"
        super().__init__(nombre="WoS", df_fuente=df_fuente)

    def get_url(self, index):
        id = self.df_fuente.iloc[index][self.map_columnas["ID"]]
        return self.url_template.format(id=id)

    def buscar_dois_en_api(self, dois: List[Dict]) -> pd.DataFrame:
        self.api.search_by_DOI_list(dois)
        records: List[Dict] = self.api.records

        record_data = pd.DataFrame(columns=["DOI", "URL"])

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

                record_data.loc[len(record_data)] = data

            except:
                pass

        return record_data


class BaseDatosCitasPerdidasScopus(BaseDatosCitasPerdidas):
    def __init__(self, df_fuente: pd.DataFrame) -> None:
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

        super().__init__(nombre="Scopus", df_fuente=df_fuente)

    def get_url(self, index):
        eid = self.df_fuente.iloc[index][self.map_columnas["ID"]]
        return self.url_template.format(id=eid)

    def buscar_dois_en_api(self, dois: List[str]) -> pd.DataFrame:
        self.api.search_by_DOI_list(dois)

        api_results: List[dict] = self.api.results

        data_list = pd.DataFrame(columns=["DOI", "URL"])

        for result in api_results:
            data = {}

            data["DOI"] = result.get("prism:doi", None)
            eid = result.get("eid", None)
            data["URL"] = self.url_template.format(id=eid)

            data_list.loc[len(data_list)] = data

        return data_list


def buscar_citas_perdidas(citas_wos: pd.DataFrame, citas_scopus: pd.DataFrame) -> None:
    bd_wos = BaseDatosCitasPerdidasWoS(df_fuente=citas_wos)
    bd_scopus = BaseDatosCitasPerdidasScopus(df_fuente=citas_scopus)

    citas_perdidas = CitasPerdidas(bds=[bd_wos, bd_scopus])
    citas_perdidas.comparar()
    citas_perdidas.generar_excel()
    pass
