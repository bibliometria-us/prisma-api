import copy
from datetime import datetime
from typing import Any, Dict

import pandas as pd
from db.conexion import BaseDatos
from routes.carga.editor.registro_cambios_editor import RegistroCambiosEditorAtributos
from routes.carga.fuente.registro_cambios_fuente import (
    RegistroCambiosFuenteAtributos,
    RegistroCambiosFuenteColeccion,
    RegistroCambiosFuenteDatos,
    RegistroCambiosFuenteIdentificadores,
)
from routes.carga.publicacion.comparar_autores import (
    ComparacionAutores,
)
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAccesoAbierto,
    DatosCargaAfiliacionesAutor,
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaDatosFuente,
    DatosCargaEditorial,
    DatosCargaFechaPublicacion,
    DatosCargaFinanciacion,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaPublicacion,
)
from routes.carga.publicacion.idus.parser import IdusParser
from routes.carga.publicacion.registro_cambios_publicacion import (
    RegistroCambiosPublicacionAtributos,
    RegistroCambiosPublicacionCantidadAutores,
    RegistroCambiosPublicacionDatos,
    RegistroCambiosPublicacionFecha,
    RegistroCambiosPublicacionFinanciacion,
    RegistroCambiosPublicacionFuente,
    RegistroCambiosPublicacionIdentificadores,
    RegistroCambiosPublicacionOpenAccess,
)
from routes.carga.registro_cambios import ProblemaCarga, RegistroCambios


class CargaPublicacion:
    """
    Clase que representa una carga de publicación de publicación generica
    """

    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:
        self.id_carga = id_carga or RegistroCambios.generar_id_carga()
        self.datos: DatosCargaPublicacion = None
        self.datos_antiguos: DatosCargaPublicacion = None
        self.auto_commit = auto_commit
        self.start_database(db)
        self.id_publicacion = 0
        self.fuente_existente = False
        self.origen = None
        self.problemas_carga: list[ProblemaCarga] = []
        self.lista_registros: list[RegistroCambios] = []

    # Decorador
    def busqueda(func):
        def wrapper(self: "CargaPublicacion", *args, **kwargs):
            if self.datos_antiguos is not None:
                return func(self, *args, **kwargs)
            else:
                return None

        return wrapper

    def start_database(self, db: BaseDatos):
        """
        Crea la conexión con la base de datos
        """
        if db:
            self.db = db
            assert self.db.autocommit == False
        else:
            self.db = BaseDatos(
                database=None, autocommit=False, keep_connection_alive=True
            )
            self.db.startConnection()
            self.db.connection.start_transaction()

    def stop_database(self):
        """
        Crea la cierra y revierte cambios en la conexión con la base de datos
        """
        self.db.rollback()
        self.db.closeConnection()

    def commit_database(self):
        """
        Crea la cierra y persiste cambios en la conexión con la base de datos
        """
        if self.auto_commit:
            self.db.commit()
            self.db.closeConnection()

    def close_database(self):
        """
        Cierra la conexión con la base de datos
        """
        self.db.closeConnection()

    def cargar_publicacion(self):
        datos_validados = self.datos.validate()
        datos_antiguos_validados = (
            self.datos_antiguos.validate() if self.datos_antiguos else True
        )
        if not (datos_validados or datos_antiguos_validados):
            raise ValueError

        self.datos.sanitize()

        self.insertar_publicacion()
        self.insertar_autores()
        self.insertar_identificadores_publicacion()
        self.insertar_datos_publicacion()
        self.insertar_fuente(tipo="fuente")
        if self.datos.fuente.coleccion.validate():
            self.insertar_fuente(tipo="coleccion")
        self.insertar_financiaciones()
        self.insertar_fechas_publicacion()
        self.insertar_valores_acceso_abierto()
        self.insertar_registros()
        self.insertar_problemas()
        self.commit_database()

    def insertar_registros(self):
        for registro in self.lista_registros:
            registro.insertar(id_carga=self.id_carga)

    def insertar_problemas(self):
        for problema in self.problemas_carga:
            problema.insertar(id_carga=self.id_carga)

    def buscar_publicacion(self):
        """
        Comprueba si una publicación está duplicada según criterios establecidos
        """

        id_publicacion = None
        for identificador in self.datos.identificadores:
            # DUDA: Se comprueba sólo el id y no el tipo de id (o fuente)
            # Se buscan las publicaciones por el o los identificadores (DOI, ID Scopus, ID WoS...) localizados en la carga
            query = f"""
                    SELECT p.idPublicacion, p.tipo, p.titulo, p.agno as año_publicacion, p.origen FROM prisma.p_publicacion p
                    LEFT JOIN prisma.p_identificador_publicacion idp ON idp.idPublicacion = p.idPublicacion
                    WHERE idp.valor = %(valor)s AND idp.tipo = %(tipo)s
                    GROUP BY p.idPublicacion
                    """
            params = {"valor": identificador.valor, "tipo": identificador.tipo}

            self.db.ejecutarConsulta(query, params)
            id_publicacion = self.db.get_first_cell()

            if id_publicacion:
                self.id_publicacion = id_publicacion
                break

        datos_publicacion = None

        if id_publicacion:
            self.id_publicacion = id_publicacion
            datos_publicacion = self.db.get_dataframe()
            datos_publicacion = datos_publicacion.iloc[0].to_dict()
            self.datos_antiguos = DatosCargaPublicacion().from_id_publicacion(
                self.id_publicacion, self.db
            )
            if not self.datos_antiguos.validate():
                return None

            self.datos_antiguos.sanitize()

        return datos_publicacion

    def comparar_atributos_publicacion(self, publicacion_antigua):
        """
        Si se ha encontrado una publicación existente con identificadores comunes,
        comparar los atributos entre sí.
        """

        tipos_atributo = ["tipo", "año_publicacion"]  # Columnas a comprobar

        for tipo_atributo in tipos_atributo:
            nuevo_atributo = getattr(self.datos, tipo_atributo)

            registro = RegistroCambiosPublicacionAtributos(
                id=self.id_publicacion,
                atributo=tipo_atributo,
                valor=nuevo_atributo,
                origen=self.origen,
                bd=self.db,
            )

            if not publicacion_antigua:
                self.lista_registros.append(registro)
                continue

            antiguo_atributo = publicacion_antigua[tipo_atributo]

            if antiguo_atributo != nuevo_atributo:
                problema = registro.detectar_conflicto(valor_actual=antiguo_atributo)
                if problema:
                    self.problemas_carga.append(problema)

    def insertar_publicacion(self):
        """
        Inserta la publicación en base de dato
        DUDA: esto es lo único que se almacena de la publicación en Prisma?
        """
        publicacion_antigua = self.buscar_publicacion()

        if not publicacion_antigua:
            query = """INSERT INTO prisma.p_publicacion (tipo, titulo, agno, origen, validado)
                        VALUES (%(tipo)s, %(titulo)s, %(agno)s, %(origen)s, %(validado)s)"""

            params = {
                "tipo": self.datos.tipo,
                "titulo": self.datos.titulo,
                "agno": self.datos.año_publicacion,
                "origen": self.origen,
                "validado": 3,
            }

            self.db.ejecutarConsulta(query, params)
            self.id_publicacion = self.db.last_id

        self.comparar_atributos_publicacion(publicacion_antigua=publicacion_antigua)

    @busqueda
    def comparar_autores(self, registro: RegistroCambiosPublicacionCantidadAutores):

        antiguos_autores = ", ".join(
            f"{key}: {value}"
            for key, value in self.datos_antiguos.contar_autores_agrupados().items()
        )

        problema = registro.detectar_conflicto(valor_actual=antiguos_autores)

        if problema:
            self.problemas_carga.append(problema)
            return problema

        return True

    def insertar_autores(self):
        """
        Inserta los autores de una publicación.
        """
        nuevos_autores = ", ".join(
            f"{key}: {value}"
            for key, value in self.datos.contar_autores_agrupados().items()
        )

        registro = RegistroCambiosPublicacionCantidadAutores(
            id=self.id_publicacion,
            valor=nuevos_autores,
            origen=self.origen,
            bd=self.db,
        )

        if self.comparar_autores(registro):
            return None

        for autor in self.datos.autores:
            self.insertar_autor(autor)

        self.lista_registros.append(registro)

    def buscar_id_investigador(self, autor: DatosCargaAutor):
        # TODO: Plantear que la id de investigador de autor se actualice siempre
        if not autor.ids:
            return 0

        where = " OR ".join(
            f"(idi.tipo = '{identificador.tipo}' AND idi.valor = '{identificador.valor}')"
            for identificador in autor.ids
        )

        query = f"""
                SELECT i.idInvestigador FROM prisma.i_investigador i
                LEFT JOIN prisma.i_identificador_investigador idi ON idi.idInvestigador = i.idInvestigador
                WHERE {where}
                """

        self.db.ejecutarConsulta(query)
        id_investigador = self.db.get_first_cell()

        return id_investigador or 0

    ## INSERCIÓN DE AUTORES
    def insertar_autor(self, autor: DatosCargaAutor):

        query = """INSERT INTO prisma.p_autor (orden,firma, rol, idPublicacion, idInvestigador, contacto)
                    VALUES (%(orden)s, %(firma)s, %(rol)s, %(idPublicacion)s, %(idInvestigador)s, %(contacto)s)
                """
        params = {
            "orden": autor.orden,
            "firma": autor.firma,
            "rol": autor.tipo,
            "contacto": autor.contacto,
            "idPublicacion": self.id_publicacion,
            "idInvestigador": self.buscar_id_investigador(autor),
        }

        self.db.ejecutarConsulta(query, params)
        id_autor = self.db.last_id

        self.insertar_afiliaciones_autor(autor, id_autor)

    ## INSERCIÓN DE AFILIACIONES DE AUTORES
    def insertar_afiliaciones_autor(self, autor: DatosCargaAutor, id_autor: int):
        for afiliacion in autor.afiliaciones:
            self.insertar_afiliacion_autor(afiliacion, id_autor)

    def insertar_afiliacion_autor(
        self, afiliacion: DatosCargaAfiliacionesAutor, id_autor
    ):
        afiliacion_id = self.buscar_afiliacion(afiliacion)

        # Si no existe la afiliación, crear una nueva y obtener su ID para vincularla al autor
        if not afiliacion_id:
            query = """INSERT INTO prisma.p_afiliacion (afiliacion, id_ror, pais)
                        VALUES (%(afiliacion)s, %(id_ror)s, %(pais)s)
                    """
            params = {
                "afiliacion": afiliacion.nombre,
                "id_ror": afiliacion.ror_id,
                "pais": afiliacion.pais,
            }

            self.db.ejecutarConsulta(query, params)
            afiliacion_id = self.db.last_id

        query = """INSERT INTO prisma.p_autor_afiliacion (autor_id, afiliacion_id)
                    VALUES (%(idAutor)s, %(nombre)s)
                """
        params = {"idAutor": id_autor, "nombre": afiliacion_id}

        self.db.ejecutarConsulta(query, params)

    def buscar_afiliacion(self, afiliacion: DatosCargaAfiliacionesAutor):
        query = """SELECT id FROM prisma.p_afiliacion WHERE id_ror = %(id_ror)s"""
        params = {"id_ror": afiliacion.ror_id}

        self.db.ejecutarConsulta(query, params)
        if self.db.result:
            id_afiliacion = self.db.get_first_cell()
            return id_afiliacion

    def insertar_identificadores_publicacion(self):
        for identificador in self.datos.identificadores:
            self.insertar_identificador_publicacion(identificador)

    @busqueda
    def buscar_identificador_publicacion(
        self,
        identificador: DatosCargaIdentificadorPublicacion,
        registro: RegistroCambiosPublicacionIdentificadores,
    ) -> bool:
        if identificador not in self.datos_antiguos.identificadores:
            return False

        problema = registro.detectar_conflicto(valor_actual=identificador.valor)

        if problema:
            self.problemas_carga.append(problema)

        return True

    def insertar_identificador_publicacion(
        self, identificador: DatosCargaIdentificadorPublicacion
    ):
        registro = RegistroCambiosPublicacionIdentificadores(
            id=self.id_publicacion,
            tipo_identificador=identificador.tipo,
            valor=identificador.valor,
            origen=self.origen,
            bd=self.db,
        )

        if self.buscar_identificador_publicacion(identificador, registro):
            return None

        query = """
                INSERT INTO prisma.p_identificador_publicacion (idPublicacion, tipo, valor, origen)
                VALUES (%(idPublicacion)s, %(tipo)s, %(valor)s, %(origen)s)
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": identificador.tipo,
            "valor": identificador.valor,
            "origen": self.origen,
        }

        self.db.ejecutarConsulta(query, params)

        self.lista_registros.append(registro)

    def insertar_datos_publicacion(self):
        for dato in self.datos.datos:
            self.insertar_dato_publicacion(dato)

    @busqueda
    def buscar_dato_publicacion(
        self, dato: DatosCargaDatoPublicacion, registro: RegistroCambiosPublicacionDatos
    ) -> bool:
        if dato not in self.datos_antiguos.datos:
            return False

        problema = registro.detectar_conflicto(valor_actual=dato.valor)

        if problema:
            self.problemas_carga.append(problema)

        return True

    def insertar_dato_publicacion(self, dato: DatosCargaDatoPublicacion):
        registro = RegistroCambiosPublicacionDatos(
            id=self.id_publicacion,
            tipo_dato=dato.tipo,
            valor=dato.valor,
            origen=self.origen,
            bd=self.db,
        )

        if self.buscar_dato_publicacion(dato, registro):
            return None

        query = """
                INSERT INTO prisma.p_dato_publicacion (idPublicacion, tipo, valor)
                VALUES (%(idPublicacion)s, %(tipo)s, %(valor)s)
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": dato.tipo,
            "valor": dato.valor,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)

    @busqueda
    def buscar_fuente(self, tipo: str) -> int:
        if tipo == "fuente":
            fuente = self.datos_antiguos.fuente
        if tipo == "coleccion":
            fuente = self.datos_antiguos.fuente.coleccion

        if fuente.id_fuente:
            return fuente.id_fuente

        return 0

    def comparar_atributos_fuente(self, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
            fuente_antigua = self.datos_antiguos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion
            fuente_antigua = self.datos_antiguos.fuente.coleccion
        nombres_atributo = ["tipo"]
        for nombre_atributo in nombres_atributo:
            atributo = getattr(fuente, nombre_atributo)
            atributo_antiguo = getattr(fuente_antigua, nombre_atributo)

            registro = RegistroCambiosFuenteAtributos(
                id=fuente.id_fuente,
                atributo=nombre_atributo,
                valor=atributo,
                origen=self.origen,
                bd=self.db,
            )

            problema = registro.detectar_conflicto(valor_actual=atributo_antiguo)
            if problema:
                self.problemas_carga.append(problema)
                return None

            self.lista_registros.append(registro)

    def insertar_fuente(self, tipo: str = "fuente") -> int:
        id_fuente = self.buscar_fuente(tipo=tipo)

        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion
        if not id_fuente:
            query = """INSERT INTO prisma.p_fuente (tipo, titulo, editorial, origen)
                VALUES (%(tipo)s, %(titulo)s, %(editorial)s, %(origen)s)"""

            editorial = fuente.editoriales[0].nombre if fuente.editoriales else None

            params = {
                "tipo": fuente.tipo,
                "titulo": fuente.titulo,
                "editorial": editorial,
                "origen": self.origen,
            }

            self.db.ejecutarConsulta(query, params)
            id_fuente = self.db.last_id
            fuente.id_fuente = id_fuente

            if tipo == "fuente":
                self.insertar_id_fuente_publicacion()
            if tipo == "coleccion":
                self.insertar_coleccion_publicacion()

        else:
            fuente.id_fuente = id_fuente
            self.comparar_atributos_fuente(tipo=tipo)

        self.insertar_datos_fuente(tipo=tipo)
        self.insertar_identificadores_fuente(tipo=tipo)
        self.insertar_editoriales_fuente(tipo=tipo)

        return id_fuente

    def insertar_id_fuente_publicacion(self):
        registro = RegistroCambiosPublicacionFuente(
            id=self.id_publicacion,
            valor=self.datos.fuente.id_fuente,
            origen=self.origen,
            bd=self.db,
        )

        query_insertar_id_fuente_publicacion = "UPDATE prisma.p_publicacion SET idFuente = %(idFuente)s WHERE idPublicacion = %(idPublicacion)s"
        params_insertar_id_fuente_publicacion = {
            "idFuente": self.datos.fuente.id_fuente,
            "idPublicacion": self.id_publicacion,
        }

        self.db.ejecutarConsulta(
            query_insertar_id_fuente_publicacion, params_insertar_id_fuente_publicacion
        )

        self.lista_registros.append(registro)

    def insertar_coleccion_publicacion(self):
        registro = RegistroCambiosFuenteColeccion(
            id=self.id_publicacion,
            valor=self.datos.fuente.coleccion.id_fuente,
            origen=self.origen,
            bd=self.db,
        )

        query_insertar_coleccion_publicacion = """
            INSERT INTO prisma.p_dato_fuente (idFuente, tipo, valor)
            VALUES (%(idFuente)s, 'coleccion', %(idColeccion)s)
            """
        params_insertar_coleccion_publicacion = {
            "idFuente": self.datos.fuente.id_fuente,
            "idColeccion": self.datos.fuente.coleccion.id_fuente,
        }

        self.db.ejecutarConsulta(
            query_insertar_coleccion_publicacion, params_insertar_coleccion_publicacion
        )

        self.lista_registros.append(registro)

    def insertar_identificadores_fuente(self, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion
        for identificador in fuente.identificadores:
            self.insertar_identificador_fuente(identificador, tipo=tipo)

    @busqueda
    def buscar_identificador_fuente(
        self,
        identificador: DatosCargaIdentificadorFuente,
        registro: RegistroCambiosFuenteIdentificadores,
        tipo: str,
    ):
        if tipo == "fuente":
            fuente_antigua = self.datos_antiguos.fuente
        if tipo == "coleccion":
            fuente_antigua = self.datos_antiguos.fuente.coleccion

        for identificador_antiguo in fuente_antigua.identificadores:
            if identificador_antiguo.valor == identificador.valor:
                return True

    def insertar_identificador_fuente(
        self, identificador: DatosCargaIdentificadorFuente, tipo: str
    ):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        registro = RegistroCambiosFuenteIdentificadores(
            id=fuente.id_fuente,
            tipo_identificador=identificador.tipo,
            valor=str(identificador),
            origen=self.origen,
            bd=self.db,
        )
        if self.buscar_identificador_fuente(identificador, registro, tipo=tipo):
            return None

        query = """
                REPLACE INTO prisma.p_identificador_fuente (idFuente, tipo, valor, origen)
                VALUES (%(idFuente)s, %(tipo)s, %(valor)s, %(origen)s)
                """
        params = {
            "idFuente": fuente.id_fuente,
            "tipo": identificador.tipo,
            "valor": identificador.valor,
            "origen": self.origen,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)

    def insertar_datos_fuente(self, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        for dato in fuente.datos:
            self.insertar_dato_fuente(dato, tipo=tipo)

    @busqueda
    def buscar_dato_fuente(self, dato: DatosCargaDatosFuente, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion
        for dato_antiguo in fuente.datos:
            if dato_antiguo.tipo == dato.tipo:
                return dato_antiguo

        return None

    def comparar_dato_fuente(
        self, dato: DatosCargaDatosFuente, registro: RegistroCambiosFuenteDatos
    ):
        problema = registro.detectar_conflicto(valor_actual=dato.valor)
        if problema:
            self.problemas_carga.append(problema)

    def insertar_dato_fuente(self, dato: DatosCargaDatosFuente, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        dato_antiguo = self.buscar_dato_fuente(dato, tipo=tipo)

        registro = RegistroCambiosFuenteDatos(
            id=fuente.id_fuente,
            tipo_dato=dato.tipo,
            valor=dato.valor,
            origen=self.origen,
            bd=self.db,
        )

        if dato_antiguo:
            self.comparar_dato_fuente(dato, registro)
            return None

        query = """
                INSERT INTO prisma.p_dato_fuente (idFuente, tipo, valor)
                VALUES (%(idFuente)s, %(tipo)s, %(valor)s)
                """
        params = {
            "idFuente": self.datos.fuente.id_fuente,
            "tipo": dato.tipo,
            "valor": dato.valor,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)

    def insertar_editoriales_fuente(self, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion
        for editorial in fuente.editoriales:
            self.insertar_editorial_fuente(editorial, tipo=tipo)

    @busqueda
    def buscar_editorial(
        self, editorial: DatosCargaEditorial, tipo: str
    ) -> DatosCargaEditorial:
        if tipo == "fuente":
            fuente_antigua = self.datos_antiguos.fuente
        if tipo == "coleccion":
            fuente_antigua = self.datos_antiguos.fuente.coleccion

        for editorial_antigua in fuente_antigua.editoriales:
            if editorial_antigua.nombre == editorial.nombre:
                return editorial_antigua

        return None

    def insertar_editorial_fuente(self, editorial: DatosCargaEditorial, tipo: str):
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        editorial_antigua = self.buscar_editorial(editorial, tipo=tipo)

        if not editorial_antigua:
            query = """
                    INSERT INTO prisma.p_editor (nombre, url, pais, tipo)
                    VALUES (%(nombre)s, %(url)s, %(pais)s, %(tipo)s)
                    """
            params = {
                "nombre": editorial.nombre,
                "url": editorial.url,
                "pais": editorial.pais,
                "tipo": editorial.tipo,
            }

            self.db.ejecutarConsulta(query, params)
            id_editor = self.db.last_id

            for atributo, valor in params.items():
                registro = RegistroCambiosEditorAtributos(
                    id=id_editor,
                    atributo=atributo,
                    valor=valor,
                    origen=self.origen,
                    bd=self.db,
                )
                self.lista_registros.append(registro)

        else:
            id_editor = editorial_antigua.id_editor

        query_insertar_editorial_fuente = """
                        INSERT INTO prisma.p_dato_fuente (idFuente, tipo, valor)
                        VALUES (%(idFuente)s, 'editorial', %(idEditor)s)
                        """
        params_insertar_editorial_fuente = {
            "idFuente": fuente.id_fuente,
            "idEditor": id_editor,
        }

        self.db.ejecutarConsulta(
            query_insertar_editorial_fuente, params_insertar_editorial_fuente
        )

    def insertar_coleccion(self):
        if not self.datos.fuente.coleccion:
            return None

        copia_fuente = copy.deepcopy(self.datos.fuente)
        self.datos.fuente = self.datos.fuente.coleccion

        id_coleccion = self.insertar_fuente()

        self.datos.fuente = copia_fuente

        if id_coleccion:
            self.vincular_coleccion(id_coleccion)

    def vincular_coleccion(self, id_coleccion: int):
        query = """
                INSERT INTO prisma.p_dato_fuente (idFuente, tipo, valor)
                VALUES (%(idFuente)s, 'coleccion', %(idColeccion)s)
                """
        params = {
            "idFuente": self.datos.fuente.id_fuente,
            "idColeccion": id_coleccion,
        }
        self.db.ejecutarConsulta(query, params)

    def insertar_fechas_publicacion(self):
        # Insertar fecha de inserción como fecha actual
        if not self.datos_antiguos:
            fecha_actual = datetime.now()
            fecha_insercion = DatosCargaFechaPublicacion(
                agno=fecha_actual.year,
                mes=fecha_actual.month,
                dia=fecha_actual.day,
                tipo="insercion",
            )
            self.datos.fechas_publicacion.append(fecha_insercion)

        for fecha in self.datos.fechas_publicacion:
            self.insertar_fecha_publicacion(fecha)

    @busqueda
    def buscar_fecha_publicacion(
        self,
        fecha: DatosCargaFechaPublicacion,
        registro: RegistroCambiosPublicacionFecha,
    ):
        if fecha.tipo == "insercion":
            return True

        fecha_antigua = next(
            (
                fecha_antigua
                for fecha_antigua in self.datos_antiguos.fechas_publicacion
                if fecha_antigua.tipo == fecha.tipo
            ),
            None,
        )

        if fecha_antigua:
            problema = registro.detectar_conflicto(valor_actual=str(fecha_antigua))
            if problema:
                self.problemas_carga.append(problema)
            return True

        return False

    def insertar_fecha_publicacion(self, fecha: DatosCargaFechaPublicacion):

        registro = RegistroCambiosPublicacionFecha(
            id=self.id_publicacion,
            tipo_fecha=fecha.tipo,
            valor=str(fecha),
            origen=self.origen,
            bd=self.db,
        )

        if self.buscar_fecha_publicacion(fecha, registro):
            return None

        query = """
                INSERT INTO prisma.p_fecha_publicacion (idPublicacion, tipo, dia, mes, agno)
                VALUES (%(idPublicacion)s, %(tipo)s, %(dia)s, %(mes)s, %(agno)s)
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": fecha.tipo,
            "dia": fecha.dia,
            "mes": fecha.mes,
            "agno": fecha.agno,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)

    def insertar_financiaciones(self):
        for financiacion in self.datos.financiacion:
            self.insertar_financiacion(financiacion)

    @busqueda
    def buscar_financiacion(
        self,
        financiacion: DatosCargaFinanciacion,
        registro: RegistroCambiosPublicacionFinanciacion,
    ):
        if financiacion in self.datos_antiguos.financiacion:
            self.comparar_financiacion(registro, financiacion)
            return True

        return False

    def comparar_financiacion(
        self,
        registro: RegistroCambiosPublicacionFinanciacion,
        financiacion: DatosCargaFinanciacion,
    ):
        problema = registro.detectar_conflicto(valor_actual=financiacion.proyecto)
        if problema:
            self.problemas_carga.append(problema)

    def insertar_financiacion(self, financiacion: DatosCargaFinanciacion):
        registro = RegistroCambiosPublicacionFinanciacion(
            id=self.id_publicacion,
            financiacion=financiacion.proyecto,
            origen=self.origen,
            bd=self.db,
        )

        if self.buscar_financiacion(financiacion, registro):
            return None

        query = """
            INSERT INTO prisma.p_financiacion (codigo, agencia, publicacion_id)
            VALUES (%(codigo)s, %(agencia)s, %(publicacion_id)s)
            """
        params = {
            "codigo": financiacion.proyecto,
            "agencia": financiacion.agencia,
            "publicacion_id": self.id_publicacion,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)

    def insertar_valores_acceso_abierto(self):
        for acceso_abierto in self.datos.acceso_abierto:
            self.insertar_acceso_abierto(acceso_abierto)

    @busqueda
    def buscar_acceso_abierto(
        self,
        acceso_abierto: DatosCargaAccesoAbierto,
        registro: RegistroCambiosPublicacionOpenAccess,
    ):
        acceso_abierto_antiguo = next(
            (
                acceso_abierto_antiguo
                for acceso_abierto_antiguo in self.datos_antiguos.acceso_abierto
                if str(acceso_abierto_antiguo) == str(acceso_abierto)
            ),
            None,
        )

        if acceso_abierto_antiguo:
            problema = registro.detectar_conflicto(
                valor_actual=str(acceso_abierto_antiguo)
            )
            if problema:
                self.problemas_carga.append(problema)
            return True

    def insertar_acceso_abierto(self, acceso_abierto: DatosCargaAccesoAbierto):
        registro = RegistroCambiosPublicacionOpenAccess(
            id=self.id_publicacion,
            tipo=acceso_abierto.origen,
            valor=str(acceso_abierto),
            origen=self.origen,
            bd=self.db,
        )

        if self.buscar_acceso_abierto(acceso_abierto, registro):
            return None

        query = """
                INSERT INTO prisma.p_acceso_abierto (publicacion_id, valor, origen)
                VALUES (%(publicacion_id)s, %(valor)s, %(origen)s)
                """
        params = {
            "publicacion_id": self.id_publicacion,
            "valor": acceso_abierto.valor,
            "origen": acceso_abierto.origen,
        }

        self.db.ejecutarConsulta(query, params)
        self.lista_registros.append(registro)
