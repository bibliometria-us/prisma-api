from config import local_config
from db.conexion import BaseDatos
from routes.carga.carga import Carga
from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaAreaInvestigador,
    DatosCargaCategoriaInvestigador,
    DatosCargaCentroInvestigador,
    DatosCargaCeseInvestigador,
    DatosCargaContratoInvestigador,
    DatosCargaDepartamentoInvestigador,
    DatosCargaInvestigador,
)
from routes.carga.investigador.exception import ErrorCargaInvestigador
from routes.carga.investigador.investigador.buscar_investigador import (
    BuscarInvestigador,
)
from routes.carga.investigador.investigador.registro_cambios_investigador import (
    RegistroCambiosInvestigadorAtributos,
    RegistroCambiosInvestigadorCese,
    RegistroCambiosInvestigadorContrato,
    RegistroCambiosNuevoInvestigador,
)
from routes.carga.registro_cambios import ProblemaCarga


class CargaInvestigador(Carga):
    def __init__(
        self,
        db: BaseDatos = None,
        id_carga=None,
        auto_commit=True,
        datos: DatosCargaInvestigador = None,
    ) -> None:
        super().__init__(db, id_carga, auto_commit)
        self.origen = "RRHH"
        self.datos: DatosCargaInvestigador = datos
        self.datos_antiguos: DatosCargaInvestigador = None
        self.advertencias_carga = []

    def cargar_investigador(self):
        self.datos.sanitize()
        self.datos.close()

        self.buscar_investigador()

        if self.datos.get_last_contrato().es_virtual():
            self._cargar_investigador_virtual()
            return

        self._cargar_investigador()

    def _cargar_investigador(self):
        self.detectar_cambio_documento_identidad()
        if not self.datos_antiguos:
            self.insertar_investigador()
        else:
            self.actualizar_investigador()

        self.insertar_registros()
        self.insertar_problemas()

    def _cargar_investigador_virtual(self):
        if self.datos_antiguos:
            self.actualizar_estado_investigador()

        self.insertar_registros()
        self.insertar_problemas()

    def insertar_investigador(self):
        self.insertar_datos_contrato()
        self.guardar_investigador()

    def insertar_datos_contrato(self):
        contrato = self.datos.get_last_contrato()
        self.insertar_area(contrato.area)
        self.insertar_departamento(contrato.departamento)
        self.insertar_centro(contrato.centro)
        self.insertar_categoria(contrato.categoria)

    def insertar_area(self, area: DatosCargaAreaInvestigador):
        if not self.buscar_area(area):
            query = """
                INSERT INTO i_area (idArea, nombre, idRama)
                VALUES (%(id)s, %(nombre)s, %(idRama)s)
            """
            params = {"id": area.id, "nombre": area.nombre, "idRama": 0}

            self.db.ejecutarConsulta(query, params)

    def buscar_area(self, area: DatosCargaAreaInvestigador):
        query = """
            SELECT idArea
            FROM i_area
            WHERE idArea = %(id)s
        """
        params = {"id": area.id}

        self.db.ejecutarConsulta(query, params)

        id_area = self.db.get_first_cell()

        if id_area is not None:
            return True

    def insertar_departamento(self, departamento: DatosCargaDepartamentoInvestigador):
        if not self.buscar_departamento(departamento):
            query = """
                INSERT INTO i_departamento (idDepartamento, nombre)
                VALUES (%(id)s, %(nombre)s)
            """
            params = {"id": departamento.id, "nombre": departamento.nombre}

            self.db.ejecutarConsulta(query, params)

    def buscar_departamento(self, departamento: DatosCargaDepartamentoInvestigador):
        query = """
            SELECT idDepartamento
            FROM i_departamento
            WHERE idDepartamento = %(id)s
        """
        params = {"id": departamento.id}

        self.db.ejecutarConsulta(query, params)

        id_departamento = self.db.get_first_cell()

        if id_departamento is not None:
            return True

    def insertar_centro(self, centro: DatosCargaCentroInvestigador):
        if not self.buscar_centro(centro):
            query = """
                INSERT INTO i_centro (idCentro, nombre)
                VALUES (%(id)s, %(nombre)s)
            """
            params = {"id": centro.id, "nombre": centro.nombre}

            self.db.ejecutarConsulta(query, params)

    def buscar_centro(self, centro: DatosCargaCentroInvestigador):
        query = """
            SELECT idCentro
            FROM i_centro
            WHERE idCentro = %(id)s
        """
        params = {"id": centro.id}

        self.db.ejecutarConsulta(query, params)

        id_centro = self.db.get_first_cell()

        if id_centro is not None:
            return True

    def insertar_categoria(self, categoria: DatosCargaCategoriaInvestigador):
        if not self.buscar_categoria(categoria):
            query = """
                INSERT INTO i_categoria (idCategoria, nombre, femenino, tipo_pp)
                VALUES (%(id)s, %(nombre)s, %(femenino)s, %(tipo_pp)s)
            """
            params = {
                "id": categoria.id,
                "nombre": categoria.nombre,
                "femenino": categoria.femenino,
                "tipo_pp": categoria.tipo_pp,
            }

            self.db.ejecutarConsulta(query, params)

    def buscar_categoria(self, categoria: DatosCargaCategoriaInvestigador):
        query = """
            SELECT idCategoria
            FROM i_categoria
            WHERE idCategoria = %(id)s
        """
        params = {"id": categoria.id}

        self.db.ejecutarConsulta(query, params)

        id_categoria = self.db.get_first_cell()

        if id_categoria is not None:
            return True

    def guardar_investigador(self):

        query = """
            INSERT INTO i_investigador (nombre, apellidos, email, docuIden, nacionalidad, sexo, fechaNacimiento,
            idCategoria, idArea, idDepartamento, idCentro, fechaContratacion, fechaNombramiento)
            VALUES (%(nombre)s, %(apellidos)s, %(email)s, %(docuIden)s, %(nacionalidad)s, %(sexo)s, %(fechaNacimiento)s
            , %(idCategoria)s, %(idArea)s, %(idDepartamento)s, %(idCentro)s, %(fechaContratacion)s, %(fechaNombramiento)s)
        """

        contrato = self.datos.get_last_contrato()
        params = {
            "nombre": self.datos.nombre,
            "apellidos": self.datos.apellidos,
            "email": self.datos.email,
            "docuIden": self.datos.documento_identidad,
            "nacionalidad": self.datos.nacionalidad,
            "sexo": self.datos.sexo,
            "fechaNacimiento": self.datos.fecha_nacimiento,
            "idCategoria": contrato.categoria.id,
            "idArea": contrato.area.id,
            "idDepartamento": contrato.departamento.id,
            "idCentro": contrato.centro.id,
            "fechaContratacion": contrato.fecha_contratacion,
            "fechaNombramiento": contrato.fecha_nombramiento,
        }

        self.db.ejecutarConsulta(query, params)
        if not self.db.error:
            self.datos.id = self.db.last_id
            registro = RegistroCambiosNuevoInvestigador(
                id=self.datos.id,
                origen=self.origen,
                id_investigador=self.datos.id,
                id_carga=self.id_carga,
                bd=self.db,
            )
            self.lista_registros.append(registro)

    def detectar_cambio_documento_identidad(self):
        query = """
            SELECT idInvestigador, docuIden
            FROM i_investigador
            WHERE email = %(email)s"""

        params = {"email": self.datos.email}
        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        if len(df) > 0:
            docu_iden_db = df["docuIden"][0]
            id_investigador_db = int(df["idInvestigador"][0])
            self.datos.id = id_investigador_db

            if docu_iden_db != self.datos.documento_identidad:
                self.actualizar_atributo_investigador(
                    atributo="docuIden",
                    valor_antiguo=docu_iden_db,
                    valor_nuevo=self.datos.documento_identidad,
                    sobrescribir=True,
                )
                if self.db.error:
                    raise ErrorCargaInvestigador(
                        f"El investigador {local_config.prisma_url}/investigador/{id_investigador_db} está posiblemente duplicado por un cambio de documento de identidad."
                    )

                return True

    def actualizar_investigador(self):
        self.insertar_datos_contrato()
        self.actualizar_atributos_investigador()
        self.actualizar_estado_investigador()

    def actualizar_atributos_investigador(self):
        dict_datos = self.datos.to_dict()
        dict_datos_antiguos = self.datos_antiguos.to_dict()

        subset_atributos = {
            "nombre": "nombre",
            "apellidos": "apellidos",
            "email": "email",
            "nacionalidad": "nacionalidad",
            "sexo": "sexo",
            "fecha_nacimiento": "fechaNacimiento",
        }

        for atributo in subset_atributos.keys():
            valor_nuevo = dict_datos[atributo]
            valor_antiguo = dict_datos_antiguos[atributo]

            if valor_nuevo != valor_antiguo:
                self.actualizar_atributo_investigador(
                    subset_atributos[atributo], valor_nuevo, valor_antiguo
                )

        pass

    def actualizar_atributo_investigador(
        self, atributo, valor_nuevo, valor_antiguo, sobrescribir=False
    ):
        registro = RegistroCambiosInvestigadorAtributos(
            id=self.datos.id,
            atributo=atributo,
            valor=valor_nuevo,
            valor_antiguo=valor_antiguo,
            origen=self.origen,
            id_carga=self.id_carga,
            bd=self.db,
        )

        if valor_antiguo and not sobrescribir:
            registro.buscar_ultimo_registro(valor_actual=valor_antiguo)
            problema: ProblemaCarga = registro.crear_problema()
            self.problemas_carga.append(problema)
            return

        query = f"""
            UPDATE i_investigador
            SET {atributo} = %(valor)s
            WHERE idInvestigador = %(id)s
        """
        params = {"valor": valor_nuevo, "id": self.datos.id}

        self.db.ejecutarConsulta(query, params)
        if not self.db.error:
            self.lista_registros.append(registro)

    def actualizar_estado_investigador(self):
        esta_activo_actualmente = self.datos_antiguos.es_investigador_activo()
        es_nuevo_contracto_activo = self.datos.es_investigador_activo()

        if esta_activo_actualmente and not es_nuevo_contracto_activo:
            self.cesar_investigador()

        if not esta_activo_actualmente and es_nuevo_contracto_activo:
            self.alta_investigador()

        if esta_activo_actualmente and es_nuevo_contracto_activo:
            self.actualizar_contrato_investigador()

    def cesar_investigador(self):
        contrato = self.datos.get_last_contrato()

        if self.datos.estados[0] == "No vigente":
            contrato.cese.tipo = "EN"
            contrato.cese.valor = "Fin del periodo de contratación"
            contrato.cese.fecha = contrato.fecha_fin_contratacion

        self.guardar_cese(contrato.cese)

    def guardar_cese(self, cese: DatosCargaCeseInvestigador):
        registro = RegistroCambiosInvestigadorCese(
            id=self.datos.id,
            origen=self.origen,
            id_carga=self.id_carga,
            bd=self.db,
        )

        self.guardar_motivo_cese(cese)

        query = """
            INSERT INTO i_fecha_cese (idInvestigador, idMotivo, fechaCese)
            VALUES (%(idInvestigador)s, %(idMotivo)s, %(fechaCese)s)
        """
        params = {
            "idInvestigador": self.datos.id,
            "idMotivo": cese.tipo,
            "fechaCese": cese.fecha,
        }

        self.db.ejecutarConsulta(query, params)

        if not self.db.error:
            self.lista_registros.append(registro)

    def guardar_motivo_cese(self, cese: DatosCargaCeseInvestigador):
        query = """
            INSERT IGNORE INTO i_motivo_cese (idMotivo, nombre)
            VALUES (%(id)s, %(nombre)s)
        """

        params = {"id": cese.tipo, "nombre": cese.valor}
        self.db.ejecutarConsulta(query, params)

    def alta_investigador(self):
        contrato = self.datos.get_last_contrato()

        registro = RegistroCambiosInvestigadorContrato(
            id=self.datos.id,
            origen=self.origen,
            id_carga=self.id_carga,
            bd=self.db,
            tipo="nuevo",
        )

        self.guardar_datos_contrato(contrato)

        if not self.db.error:
            self.lista_registros.append(registro)

    def actualizar_contrato_investigador(self):
        contrato = self.datos.get_last_contrato()
        contrato_antiguo = self.datos_antiguos.get_last_contrato()

        if contrato.es_mismo_contrato(contrato_antiguo):
            return

        registro = RegistroCambiosInvestigadorContrato(
            id=self.datos.id,
            origen=self.origen,
            id_carga=self.id_carga,
            bd=self.db,
            tipo="actualizado",
        )

        self.guardar_datos_contrato(contrato)

        if not self.db.error:
            self.lista_registros.append(registro)

    def guardar_datos_contrato(self, contrato: DatosCargaContratoInvestigador):
        query = """
            UPDATE i_investigador
            SET idCategoria = %(idCategoria)s, idArea = %(idArea)s, idDepartamento = %(idDepartamento)s,
            idCentro = %(idCentro)s, fechaContratacion = %(fechaContratacion)s, fechaNombramiento = %(fechaNombramiento)s
            WHERE idInvestigador = %(id)s
        """

        self.guardar_area(contrato.area)
        self.guardar_departamento(contrato.departamento)
        self.guardar_centro(contrato.centro)

        params = {
            "idCategoria": contrato.categoria.id,
            "idArea": contrato.area.id,
            "idDepartamento": contrato.departamento.id,
            "idCentro": contrato.centro.id,
            "fechaContratacion": contrato.fecha_contratacion,
            "fechaNombramiento": contrato.fecha_nombramiento,
            "id": self.datos.id,
        }
        self.db.ejecutarConsulta(query, params)

    def guardar_area(self, area: DatosCargaAreaInvestigador):
        query = """ INSERT IGNORE INTO i_area (idArea, nombre, idRama)
            VALUES (%(id)s, %(nombre)s, %(idRama)s)
        """
        params = {"id": area.id, "nombre": area.nombre, "idRama": 0}
        self.db.ejecutarConsulta(query, params)
        if self.db.rowcount > 0:
            self.advertencias_carga.append(
                f"El área {area.nombre} (id: {area.id}) no existía previamente en la base de datos y ha sido creada automáticamente en esta carga. Se recomienda revisar la rama asociada a esta área."
            )

    def guardar_departamento(self, departamento: DatosCargaDepartamentoInvestigador):
        query = """ INSERT IGNORE INTO i_departamento (idDepartamento, nombre)
            VALUES (%(id)s, %(nombre)s)
        """
        params = {"id": departamento.id, "nombre": departamento.nombre}
        self.db.ejecutarConsulta(query, params)
        if self.db.rowcount > 0:
            self.advertencias_carga.append(
                f"El departamento {departamento.nombre} (id: {departamento.id}) no existía previamente en la base de datos y ha sido creado automáticamente en esta carga."
            )

    def guardar_centro(self, centro: DatosCargaCentroInvestigador):
        query = """ INSERT IGNORE INTO i_centro (idCentro, nombre)
            VALUES (%(id)s, %(nombre)s)
        """
        params = {"id": centro.id, "nombre": centro.nombre}
        self.db.ejecutarConsulta(query, params)
        if self.db.rowcount > 0:
            self.advertencias_carga.append(
                f"El centro {centro.nombre} (id: {centro.id}) no existía previamente en la base de datos y ha sido creado automáticamente en esta carga. Se recomienda revisar la biblioteca y encargado asociados."
            )

    def buscar_investigador(self):
        busqueda = BuscarInvestigador(db=self.db)
        self.datos_antiguos = busqueda.buscar_investigador(
            documento_identidad=self.datos.documento_identidad
        )

        if self.datos_antiguos:
            self.datos.id = busqueda.datos.id
            self.datos_antiguos.close()

    def limpiar_registros_importacion(self):
        pass

    def procesar_registros(self):
        pass
