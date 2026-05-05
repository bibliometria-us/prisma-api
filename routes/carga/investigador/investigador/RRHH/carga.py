import datetime
import pprint
from db.conexion import BaseDatos
from integration.email.email import enviar_correo
from integration.readers.RRHH.RRHH import RRHHReader
from models import investigador
from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaInvestigador,
    DatosCargaContratoInvestigador,
)
from routes.carga.investigador.exception import ErrorCargaInvestigador
from routes.carga.investigador.investigador.RRHH.parser import (
    ParserCeseRRHH,
    ParserInvestigador,
    ParserCese,
    ParserInvestigadorRRHH,
)
from routes.carga.investigador.investigador import config
from config import local_config

# from routes.carga.investigador.investigador.RRHH.guardadp import RRHHGuardado
import json
from routes.carga.investigador.investigador.carga import CargaInvestigador
from routes.carga.registro_cambios import RegistroCambios
import tempfile
import pandas as pd
import tests.integration.utils.utils_investigador as utils_investigador


class ImportarInvestigadoresRRHH:
    def __init__(self) -> None:
        self.datos_investigadores: list[DatosCargaInvestigador] = []
        self.id_carga = RegistroCambios.generar_id_carga()
        self.errores_carga: list[str] = []
        self.advertencias_carga: list[str] = []
        self.db = BaseDatos(autocommit=False, keep_connection_alive=True)
        self.db.startConnection()
        self.db.start_transaction()

    def importar_investigadores_RRHH(
        self, file_paths: dict = {}, dry_run: bool = False
    ) -> None:
        self.lectura_ficheros_RRHH(file_paths=file_paths)
        self.cargar_investigadores(dry_run=dry_run)

    def lectura_ficheros_RRHH(self, file_paths: dict = {}) -> None:
        # Lectura de ficheros
        # PDI
        reader = RRHHReader(file_path=file_paths["pdi"])
        reader.set_expected_columns(config.inves_columns)
        records_pdi = reader.read(sheet_name="PDI en periodo", skiprows=1)
        # PI
        reader = RRHHReader(file_path=file_paths["pi"])
        reader.set_expected_columns(config.inves_columns)
        records_pi = reader.read(sheet_name="PI en periodo", skiprows=1)
        records_pi = [
            record for record in records_pi if record.get("TIPO_PERSONAL") == "I"
        ]
        # Cese PDI
        reader = RRHHReader(file_path=file_paths["pdi_ceses"])
        reader.set_expected_columns(config.ceses_pdi_columns)
        records_ceses_pdi = reader.read(sheet_name="PDI CESES en periodo", skiprows=1)
        # Cese PI
        reader = RRHHReader(file_path=file_paths["pi_ceses"])
        reader.set_expected_columns(config.ceses_pi_columns)
        records_ceses_pi = reader.read(sheet_name="PInv CESES en periodo", skiprows=1)
        records_ceses_pi = [
            record for record in records_ceses_pi if record.get("TIPO_PERSONAL") == "I"
        ]
        if len(records_pdi) == 0 and len(records_pi) == 0:
            raise ValueError(f"El listado de investigadores está vacío.")

        # Investigadores - PDI
        investigadores_a_guardar: dict[str, DatosCargaInvestigador] = {}

        for record in records_pdi:
            parser = ParserInvestigadorRRHH(data=record, tipo_fichero="pdi")
            investigador = parser.datos_carga_investigador
            # Se busca si existe el investigador en la lista de investigadores añadidos
            investigador_in = investigadores_a_guardar.get(
                investigador.documento_identidad
            )
            # Si existe, se añade el contrato al investigador
            if investigador_in:
                investigador_in.add_contrato(investigador.contratos[0])
            # Si no existe, se añade el investigador a la lista
            else:
                investigadores_a_guardar[investigador.documento_identidad] = (
                    investigador
                )
        # Investigadores - PI
        for record in records_pi:
            parser = ParserInvestigadorRRHH(data=record, tipo_fichero="pi")
            investigador = parser.datos_carga_investigador
            # Se busca si existe el investigador en la lista de investigadores añadidos
            investigador_in = investigadores_a_guardar.get(
                investigador.documento_identidad
            )
            # Si existe, se añade el contrato al investigador
            if investigador_in:
                investigador_in.add_contrato(investigador.contratos[0])
            # Si no existe, se añade el investigador a la lista
            else:
                investigadores_a_guardar[investigador.documento_identidad] = (
                    investigador
                )

        # Ceses - PDI
        for record in records_ceses_pdi:
            parser = ParserCeseRRHH(data=record, tipo_fichero="pdi")
            cese = parser.datos_carga_cese_investigador
            # Se busca el investigador al que pertenece el cese
            investigador_in = investigadores_a_guardar.get(cese.documento_identidad)
            # Si se encuentra el investigador, se añade el cese al contrato mas cercano
            if investigador_in:
                contrato = investigador_in.get_nearest_contrato(fecha_cese=cese.fecha)

                # si el contrato existe, se añade el cese
                if contrato:
                    contrato.set_cese(cese)
                # si el contrato no existe, se añade uno virtual con el cese
                else:
                    investigador_in.add_contrato_virtual_con_cese(cese=cese)
            # Si no existe el investigador, se añade uno virtual, con un contrato virtual al cual se añade el cese
            else:
                investigador = DatosCargaInvestigador()
                investigador.from_cese(cese)
                investigador.add_contrato_virtual_con_cese(cese=cese)
                investigadores_a_guardar[investigador.documento_identidad] = (
                    investigador
                )

        # Ceses - PI
        for record in records_ceses_pi:
            parser = ParserCeseRRHH(data=record, tipo_fichero="pi")
            cese = parser.datos_carga_cese_investigador
            # Se busca el investigador al que pertenece el cese
            investigador_in = investigadores_a_guardar.get(cese.documento_identidad)
            # Si se encuentra el investigador, se añade el cese al contrato mas cercano
            if investigador_in:
                contrato = investigador_in.get_nearest_contrato(fecha_cese=cese.fecha)

                # si el contrato existe, se añade el cese
                if contrato:
                    contrato.set_cese(cese)
                # si el contrato no existe, se añade uno virtual con el cese
                else:
                    investigador_in.add_contrato_virtual_con_cese(cese=cese)
            # Si no existe el investigador, se añade uno virtual, con un contrato virtual al cual se añade el cese
            else:
                investigador = DatosCargaInvestigador()
                investigador.from_cese(cese)
                investigador.add_contrato_virtual_con_cese(cese=cese)
                investigadores_a_guardar[investigador.documento_identidad] = (
                    investigador
                )

        # Se añaden los investigadores a la lista de investigadores a guardar
        self.datos_investigadores = list(investigadores_a_guardar.values())

    def cargar_investigadores(self, dry_run: bool = False) -> None:
        try:
            for datos_investigador in self.datos_investigadores:
                try:
                    carga_investigador = CargaInvestigador(
                        db=self.db, id_carga=self.id_carga, datos=datos_investigador
                    )
                    carga_investigador.cargar_investigador()

                    if carga_investigador.advertencias_carga:
                        self.advertencias_carga.extend(
                            carga_investigador.advertencias_carga
                        )
                except ErrorCargaInvestigador as e:
                    self.errores_carga.append(str(e))
                    continue

            self.generar_informe()
            self.generar_informe_gestion()

            if not dry_run:
                self.db.commit()

        except Exception as e:
            self.db.rollback()

            enviar_correo(
                adjuntos=[],
                asunto="Carga de RRHH. Error",
                destinatarios=local_config.email_bib,
                texto_plano="",
                texto_html=f"Error inesperado en la carga de RRHH: {str(e)}",
            )

        self.db.closeConnection()

    def generar_informe(self):
        estadisticas_nuevos_investigadores = self.estadisticas_nuevos_investigadores()

        datos_informes = {
            "nuevos_investigadores": estadisticas_nuevos_investigadores,
        }

        file_path = f"/tmp/nuevos_investigadores_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            for sheet_name, df in datos_informes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        mensaje = f"""
            Buenos días,
            <br><br>
            Se ha actualizado el personal investigador activo incluido en PRISMA ({self.cantidad_investigadores_activos()} en total) según la información proporcionada por la Unidad de Nóminas de la Universidad de Sevilla con fecha {datetime.datetime.now().strftime('%d-%m-%Y')}.
            <br><br>
            Se han incorporado {len(estadisticas_nuevos_investigadores)} personas. Adjuntamos un listado de estas personas con objeto de revisar:
            
            <ul>
              <li>Email (muy recomendable Teams para encontrarlo)</li>
              <li>Sus perfiles de investigación (recordamos que el perfil de Dialnet debe estar obligatoriamente afiliado a la US para que nos lleguen sus publicaciones)</li>
              <li>Cargar publicaciones desde WoS y Scopus en el caso de que tengan publicaciones en sus perfiles anteriores al año pasado.</li>
            </ul>
            """
        enviar_correo(
            adjuntos=[file_path],
            asunto="Carga de RRHH",
            destinatarios=local_config.email_bib,
            texto_plano="",
            texto_html=mensaje,
        )

    def generar_informe_gestion(self):
        estadisticas_documento_identidad_modificado = (
            self.estadisticas_documento_identidad_modificado()
        )
        errores_carga = pd.DataFrame({"Error": self.errores_carga})
        advertencias_carga = pd.concat(
            [
                estadisticas_documento_identidad_modificado,
                pd.DataFrame({"A revisar": self.advertencias_carga}),
            ],
            ignore_index=True,
        )

        estadisticas_ceses = self.estadisticas_ceses()
        estadisticas_contratos = self.estadisticas_contratos()

        datos_informes = {
            "errores_carga": errores_carga,
            "revision": advertencias_carga,
            "contratos": estadisticas_contratos,
            "ceses": estadisticas_ceses,
        }

        file_path = f"/tmp/revision_carga_rrhh_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            for sheet_name, df in datos_informes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        mensaje = f"""Informe de revisión de carga de RRHH. Se adjuntan ficheros para revisar errores o inconsistencias.
            """
        enviar_correo(
            adjuntos=[file_path],
            asunto="Carga de RRHH. Informe de revisión",
            destinatarios=local_config.email_bib,
            texto_plano="",
            texto_html=mensaje,
        )

    def estadisticas_nuevos_investigadores(self):
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/investigador/', ii.idInvestigador) AS 'URL Prisma',
        CONCAT(ii.apellidos, ', ', ii.nombre) AS 'Nombre',
        ii.email AS 'Email',
        ia.nombre AS 'Área',
        id.nombre AS 'Departamento',
        ic.nombre AS 'Centro',
        ica.nombre AS 'Categoría',
        ib.nombre AS 'Biblioteca'
        FROM a_registro_cambios_investigador arci
        LEFT JOIN i_investigador ii ON ii.idInvestigador = arci.id
        LEFT JOIN i_area ia ON ia.idArea = ii.idArea
        LEFT JOIN i_departamento id ON id.idDepartamento = ii.idDepartamento
        LEFT JOIN i_centro ic ON ic.idCentro = ii.idCentro
        LEFT JOIN i_categoria ica ON ica.idCategoria = ii.idCategoria   
        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
        WHERE tipo_dato = 'id_investigador'
        AND id_carga = %(id_carga)s
        ORDER BY ib.nombre, ii.apellidos, ii.nombre
        """
        params = {"id_carga": self.id_carga}

        self.db.ejecutarConsulta(query, params=params)
        df = self.db.get_dataframe()

        return df

    def estadisticas_documento_identidad_modificado(self):
        query = f"""
        SELECT
        ii.idInvestigador AS 'ID Prisma',
        CONCAT(ii.apellidos, ', ', ii.nombre) AS 'Nombre',
        ii.email AS 'Email',
        arci.valor_antiguo AS 'Documento de identidad antiguo',
        arci.valor AS 'Documento de identidad nuevo'
        FROM a_registro_cambios_investigador arci
        LEFT JOIN i_investigador ii ON ii.idInvestigador = arci.id
        WHERE tipo_dato = 'docuIden'
        AND id_carga = %(id_carga)s
        """
        params = {"id_carga": self.id_carga}

        self.db.ejecutarConsulta(query, params=params)
        df = self.db.get_dataframe()

        # Transform df into a new DataFrame with only an "Advertencia" column
        df["A revisar"] = df.apply(
            lambda row: f"Documento de identidad modificado para {row['Nombre']} (id: {row['ID Prisma']}): de {row['Documento de identidad antiguo']} a {row['Documento de identidad nuevo']}",
            axis=1,
        )
        df = df[["A revisar"]]

        return df

    def estadisticas_ceses(self):
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/investigador/', ii.idInvestigador) AS 'URL Prisma',
        CONCAT(ii.apellidos, ', ', ii.nombre) AS 'Nombre',
        ii.email AS 'Email',
        ib.nombre AS 'Biblioteca',
        imc.nombre AS 'Motivo de cese'
        FROM a_registro_cambios_investigador arci
        LEFT JOIN i_investigador ii ON ii.idInvestigador = arci.id
        LEFT JOIN i_centro ic ON ic.idCentro = ii.idCentro
        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
        LEFT JOIN i_fecha_cese icese ON icese.idInvestigador = ii.idInvestigador
        LEFT JOIN i_motivo_cese imc ON imc.idMotivo = icese.idMotivo
        WHERE tipo_dato = 'cese'
        AND id_carga = %(id_carga)s
        ORDER BY ib.nombre, ii.apellidos, ii.nombre
        """
        params = {"id_carga": self.id_carga}

        self.db.ejecutarConsulta(query, params=params)
        df = self.db.get_dataframe()

        return df

    def estadisticas_contratos(self):
        query = f"""
        SELECT
        CONCAT('{local_config.prisma_url}', '/investigador/', ii.idInvestigador) AS 'URL Prisma',
        CONCAT(ii.apellidos, ', ', ii.nombre) AS 'Nombre',
        ii.email AS 'Email',
        arci.comentario AS 'Cambio en contrato',
        ia.nombre AS 'Área',
        id.nombre AS 'Departamento',
        ic.nombre AS 'Centro',
        ica.nombre AS 'Categoría',
        ib.nombre AS 'Biblioteca'
        FROM a_registro_cambios_investigador arci
        LEFT JOIN i_investigador ii ON ii.idInvestigador = arci.id
        LEFT JOIN i_area ia ON ia.idArea = ii.idArea
        LEFT JOIN i_departamento id ON id.idDepartamento = ii.idDepartamento
        LEFT JOIN i_centro ic ON ic.idCentro = ii.idCentro
        LEFT JOIN i_categoria ica ON ica.idCategoria = ii.idCategoria
        LEFT JOIN i_biblioteca ib ON ib.idBiblioteca = ic.idBiblioteca
        WHERE tipo_dato = 'contrato'
        AND id_carga = %(id_carga)s
        ORDER BY ib.nombre, ii.apellidos, ii.nombre
        """
        params = {"id_carga": self.id_carga}

        self.db.ejecutarConsulta(query, params=params)
        df = self.db.get_dataframe()

        return df

    def cantidad_investigadores_activos(self):
        query = "SELECT COUNT(*) FROM i_investigador_activo"
        self.db.ejecutarConsulta(query)

        return self.db.get_first_cell()
