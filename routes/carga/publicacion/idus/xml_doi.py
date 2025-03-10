from datetime import datetime
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaPublicacion,
)
from routes.carga.publicacion.idus.parser import IdusParser
from lxml import etree


class xmlDoiIdus:
    def __init__(self, handle: str) -> None:
        self.handle = handle
        self.xml = ""
        self.data: DatosCargaPublicacion = None
        self.data_dict = {}
        self.api_request()
        self.generate_xml()
        self.prettify_xml()

    def api_request(self):
        parser = IdusParser(handle=self.handle)
        self.data = parser.datos_carga_publicacion
        self.data_dict = self.data.to_dict()

    def prettify_xml(self):
        parser = etree.XMLParser(remove_blank_text=True)
        xml = etree.fromstring(self.xml, parser)
        self.xml = etree.tostring(xml, pretty_print=True, encoding="unicode")

    def get_contributors(self):
        sequence = "first"
        xml = ""

        for autor in self.data.autores:
            firma = autor.firma
            name = firma.split(",")[1].strip()
            surname = firma.split(",")[0].strip()
            xml += f"""
                    <person_name sequence="{sequence}" contributor_role="author">
                        <given_name>{name}</given_name>
                        <surname>{surname}</surname>                                               
                    </person_name>
                   """
            sequence = "additional"

        return xml

    def get_date(self):
        fecha_publicacion = next(
            (
                fecha
                for fecha in self.data.fechas_publicacion
                if fecha.tipo == "publicacion"
            ),
            None,
        )

        xml = f"""
                <creation_date>
                    <month>{str(fecha_publicacion.mes)}</month>
                    <day>{str(fecha_publicacion.dia)}</day>
                    <year>{str(fecha_publicacion.agno)}</year>
                </creation_date>
                <publication_date>
                    <month>{str(fecha_publicacion.mes)}</month>
                    <day>{str(fecha_publicacion.dia)}</day>
                    <year>{str(fecha_publicacion.agno)}</year>
                </publication_date>
            """

        return xml

    def generate_xml(self):
        xml = f"""
        <doi_batch xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xsi:schemaLocation="http://www.crossref.org/schema/5.3.0 https://www.crossref.org/schemas/crossref5.3.0.xsd"
                       xmlns="http://www.crossref.org/schema/5.3.0" xmlns:jats="http://www.ncbi.nlm.nih.gov/JATS1"
                       xmlns:fr="http://www.crossref.org/fundref.xsd" xmlns:ai="http://www.crossref.org/AccessIndicators.xsd" xmlns:mml="http://www.w3.org/1998/Math/MathML" version="5.3.0">
                                
            <head>
                <doi_batch_id>"idus:{self.handle}"</doi_batch_id>
                <timestamp>{datetime.now().strftime('%Y%m%d%H%M%S')}</timestamp>
                <depositor>
                    <depositor_name>sevl:sevl</depositor_name>
                    <email_address>secpub5@us.es</email_address>
                </depositor>
                <registrant>Universidad de Sevilla</registrant>
            </head>                    
            <body>
                <database>
                    <database_metadata language="es">
                        <titles>
                            <title>Datasets de idUS</title>
                        </titles>
                        <institution>
                            <institution_name>Universidad de Sevilla</institution_name>
                            <institution_id type="ror">https://ror.org/03yxnpp24</institution_id>
                        </institution>
                    </database_metadata>
                    <dataset dataset_type="collection">
                        <contributors>
                            {self.get_contributors()}
                        </contributors>
                        <titles>
                            <title>{self.data.titulo}</title>
                        </titles>
                        <database_date> 
                            {self.get_date()}
                        </database_date>
                        <format mime_type="text/html"/>
                        <doi_data>
                            <doi>10.12795/{self.handle}</doi>
                            <resource>https://idus.us.es/handle/{self.handle}</resource>
                        </doi_data>
                    </dataset>
                </database>
            </body>   
        </doi_batch>
            
                    """

        self.xml = xml
