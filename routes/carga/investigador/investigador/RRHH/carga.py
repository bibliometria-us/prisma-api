import pprint
from db.conexion import BaseDatos
from integration.readers.RRHH.RRHH import RRHHReader
from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaInvestigador,
    DatosCargaContratoInvestigador,
)
from routes.carga.investigador.investigador.RRHH.parser import (
    ParserInvestigador,
    ParserCese,
)
from routes.carga.investigador.investigador import config

# from routes.carga.investigador.investigador.RRHH.guardadp import RRHHGuardado
import json
import tests.integration.utils.utils_investigador as utils_investigador


class CargaInvestigadorRRHH:
    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:
        self.origen = "RRHH"
        self.datos_investigadores: list[DatosCargaInvestigador] = []

    def cargar_investigador_RRHH(
        self,
        file_paths: dict = {},
    ) -> None:
        # Lectura de ficheros
        # PDI
        reader = RRHHReader(file_path=file_paths["pdi"])
        reader.set_expected_columns(config.inves_columns)
        records_pdi = reader.read(sheet_name="PDI en periodo", skiprows=1)
        # PI
        reader = RRHHReader(file_path=file_paths["pi"])
        reader.set_expected_columns(config.inves_columns)
        records_pi = reader.read(sheet_name="PI en periodo", skiprows=1)
        # Cese PDI
        reader = RRHHReader(file_path=file_paths["pdi_ceses"])
        reader.set_expected_columns(config.ceses_pdi_columns)
        records_ceses_pdi = reader.read(sheet_name="PDI CESES en periodo", skiprows=1)
        # Cese PI
        reader = RRHHReader(file_path=file_paths["pi_ceses"])
        reader.set_expected_columns(config.ceses_pi_columns)
        records_ceses_pi = reader.read(sheet_name="PInv CESES en periodo", skiprows=1)

        if len(records_pdi) == 0 and len(records_pi) == 0:
            raise ValueError(f"El listado de investigadores está vacío.")
        # Investigadores - PDI
        for record in records_pdi:
            parser = ParserInvestigador(data=record, tipo_fichero="pdi")
            investigador = parser.datos_carga_investigador
            # Se busca si existe el investigador en la lista de investigadores añadidos
            investigador_in = next(
                (
                    inv
                    for inv in self.datos_investigadores
                    if inv.documento_identidad == investigador.documento_identidad
                ),
                None,
            )
            # Si existe, se añade el contrato al investigador
            if investigador_in:
                investigador_in.add_contrato(investigador.get_last_contrato())
            # Si no existe, se añade el investigador a la lista
            else:
                self.datos_investigadores.append(investigador)
        # Investigadores - PI
        for record in records_pi:
            parser = ParserInvestigador(data=record, tipo_fichero="pi")
            investigador = parser.datos_carga_investigador
            # Se busca si existe el investigador en la lista de investigadores añadidos
            investigador_in = next(
                (
                    inv
                    for inv in self.datos_investigadores
                    if inv.documento_identidad == investigador.documento_identidad
                ),
                None,
            )
            # Si existe, se añade el contrato al investigador
            if investigador_in:
                investigador_in.add_contrato(investigador.get_last_contrato())
            # Si no existe, se añade el investigador a la lista
            else:
                self.datos_investigadores.append(investigador)

        # Ceses - PDI
        for record in records_ceses_pdi:
            parser = ParserCese(data=record, tipo_fichero="pdi")
            cese = parser.datos_carga_cese_investigador
            # Se busca el investigador al que pertenece el cese
            investigador_in = next(
                (
                    inv
                    for inv in self.datos_investigadores
                    if inv.documento_identidad == cese.documento_identidad
                ),
                None,
            )
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
                investigador = DatosCargaInvestigador(
                    documento_identidad=cese.documento_identidad
                )
                investigador.add_contrato_virtual_con_cese(cese=cese)
                self.datos_investigadores.append(investigador)

        # Ceses - PI
        for record in records_ceses_pi:
            parser = ParserCese(data=record, tipo_fichero="pi")
            cese = parser.datos_carga_cese_investigador
            # Se busca el investigador al que pertenece el cese
            investigador_in = next(
                (
                    inv
                    for inv in self.datos_investigadores
                    if inv.documento_identidad == cese.documento_identidad
                ),
                None,
            )
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
                investigador = DatosCargaInvestigador(
                    documento_identidad=cese.documento_identidad
                )
                investigador.add_contrato_virtual_con_cese(cese=cese)
                self.datos_investigadores.append(investigador)

        # for inv_test in self.datos_investigadores:
        #     cont_ceses = 0
        #     for contrato in inv_test.contratos:
        #         if contrato.cese.fecha is not None or contrato.cese.fecha != "":
        #             cont_ceses += 1
        #     if cont_ceses > 1:
        #         print(inv_test.to_json())

        # resultado = utils_investigador.estadisticas_datos_publicacion(
        # self.datos_investigadores
        # )
        # pprint.pp(resultado)

        # TODO: Sanitizar y validar
        # TODO: Si todo ok
        # TODO: clase de guardado

        return None
