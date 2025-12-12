import copy
from datetime import datetime
from typing import Any, Dict
import sys

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
        autores_agrupados = self.datos.contar_autores_agrupados()
        nuevos_autores = ", ".join(
            f"{key}: {value}" for key, value in autores_agrupados.items()
        )

        registro = RegistroCambiosPublicacionCantidadAutores(
            id=self.id_publicacion,
            valor=nuevos_autores,
            origen=self.origen,
            bd=self.db,
        )

        resultado_comparacion = self.comparar_autores(registro)

        # Si resultado_comparacion es None (publicación nueva) o True (sin conflictos), insertar autores
        # Solo hacer early return si hay un problema de conflicto
        # CASO 1: Hay conflicto → Registrar pero NO insertar
        if resultado_comparacion is not None and resultado_comparacion is not True:
            return None

        # CASO 2: Ya existen autores (sin conflicto) → NO insertar
        if resultado_comparacion is True:
            # Ya hay autores en la BD, no insertamos de nuevo
            return None
        # CASO 3: Publicación nueva (resultado_comparacion = None) → Insertar
        # Solo llegamos aquí si es la primera vez que se cargan autores
        for i, autor in enumerate(self.datos.autores):
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
    def buscar_fuente_en_datos_antiguos(self, tipo: str) -> int:
        """Busca la fuente en los datos antiguos de la publicación"""
        if tipo == "fuente":
            fuente = self.datos_antiguos.fuente
        if tipo == "coleccion":
            fuente = self.datos_antiguos.fuente.coleccion

        if fuente.id_fuente:
            return fuente.id_fuente
        return 0

    def buscar_fuente_en_bd(self, tipo: str) -> int:
        """Busca una fuente existente en la base de datos por sus identificadores"""
        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        # Buscar por identificadores únicos (ISSN, ISBN, etc.)
        # Los identificadores son únicos, por lo que si coinciden, es la misma fuente
        for identificador in fuente.identificadores:
            # Normalizar identificador: quitar guiones y espacios, convertir a mayúsculas
            valor_normalizado = (
                identificador.valor.replace("-", "").replace(" ", "").upper()
            )

            # Para ISSN/eISSN e ISBN/eISBN, buscar ambos tipos ya que representan la misma fuente
            tipos_busqueda = []
            if identificador.tipo.lower() in ["issn", "eissn"]:
                tipos_busqueda = ["issn", "eissn"]
            elif identificador.tipo.lower() in ["isbn", "eisbn"]:
                tipos_busqueda = ["isbn", "eisbn"]
            else:
                tipos_busqueda = [identificador.tipo]

            # Crear placeholders para la cláusula IN usando nombres
            placeholders = ", ".join(
                [f"%(tipo_{i})s" for i in range(len(tipos_busqueda))]
            )

            # Búsqueda principal: por identificador normalizado y tipos equivalentes
            query = f"""
                SELECT f.idFuente 
                FROM prisma.p_fuente f
                INNER JOIN prisma.p_identificador_fuente idf ON idf.idFuente = f.idFuente
                WHERE REPLACE(REPLACE(UPPER(idf.valor), '-', ''), ' ', '') = %(valor_normalizado)s
                AND idf.tipo IN ({placeholders})
                AND f.tipo = %(tipo_fuente)s
                LIMIT 1
            """

            # Construir diccionario de parámetros
            params = {
                "valor_normalizado": valor_normalizado,
                "tipo_fuente": fuente.tipo,
            }

            # Agregar tipos de búsqueda al diccionario
            for i, tipo in enumerate(tipos_busqueda):
                params[f"tipo_{i}"] = tipo

            self.db.ejecutarConsulta(query, params)
            id_fuente = self.db.get_first_cell()

            if id_fuente:
                return id_fuente

        return 0

    def buscar_fuente(self, tipo: str) -> int:
        """
        Busca una fuente existente:
        1. Primero en datos antiguos (si existen)
        2. Después en la base de datos por identificadores
        """
        # Intentar buscar en datos antiguos primero
        id_fuente = self.buscar_fuente_en_datos_antiguos(tipo)
        if id_fuente:
            return id_fuente

        # Si no se encuentra en datos antiguos, buscar en BD
        id_fuente = self.buscar_fuente_en_bd(tipo)
        return id_fuente

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

            self.lista_registros.append(registro)

    def insertar_fuente(self, tipo: str = "fuente") -> int:
        id_fuente = self.buscar_fuente(tipo=tipo)

        if tipo == "fuente":
            fuente = self.datos.fuente
        if tipo == "coleccion":
            fuente = self.datos.fuente.coleccion

        # Validar coherencia entre tipo de fuente e identificadores
        self.validar_coherencia_fuente_identificadores(fuente, tipo)

        if not id_fuente:
            # La fuente no existe, crearla
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
            # La fuente ya existe, usar el ID encontrado
            fuente.id_fuente = id_fuente

            # IMPORTANTE: Vincular la fuente existente a la publicación
            if tipo == "fuente":
                self.insertar_id_fuente_publicacion()
            if tipo == "coleccion":
                self.insertar_coleccion_publicacion()

            # Solo comparar atributos si hay datos antiguos
            if self.datos_antiguos:
                self.comparar_atributos_fuente(tipo=tipo)

        self.insertar_datos_fuente(tipo=tipo)
        self.insertar_identificadores_fuente(tipo=tipo)
        self.insertar_editoriales_fuente(tipo=tipo)

        return id_fuente

    def validar_coherencia_fuente_identificadores(self, fuente, tipo_contexto: str):
        """
        Valida que el tipo de fuente sea coherente con sus identificadores
        y reporta problemas si hay inconsistencias
        """
        problemas_detectados = []

        # Verificar coherencia específica por tipo
        if fuente.tipo == "libro":
            if not fuente.tiene_isbn():
                problema = ProblemaCarga(
                    elemento=f"Fuente {tipo_contexto}",
                    elemento_id=self.id_publicacion,
                    descripcion=f"Fuente tipo 'libro' sin identificadores ISBN/eISBN. "
                    f"Título: {fuente.titulo}. "
                    f"Identificadores encontrados: {[f'{id.tipo}:{id.valor}' for id in fuente.identificadores]}",
                    origen=self.origen,
                    bd=self.db,
                )
                self.problemas_carga.append(problema)

        elif fuente.tipo == "coleccion":
            if not fuente.tiene_issn():
                problema = ProblemaCarga(
                    elemento=f"Fuente {tipo_contexto}",
                    elemento_id=self.id_publicacion,
                    descripcion=f"Fuente tipo 'colección' sin identificadores ISSN/eISSN. "
                    f"Título: {fuente.titulo}. "
                    f"Identificadores encontrados: {[f'{id.tipo}:{id.valor}' for id in fuente.identificadores]}",
                    origen=self.origen,
                    bd=self.db,
                )
                self.problemas_carga.append(problema)

        # Verificar identificadores inapropiados
        if fuente.tipo == "libro" and fuente.tiene_issn():
            problema = ProblemaCarga(
                elemento=f"Fuente {tipo_contexto}",
                elemento_id=self.id_publicacion,
                descripcion=f"Fuente tipo 'libro' tiene identificadores ISSN/eISSN inapropiados. "
                f"Título: {fuente.titulo}. "
                f"ISSNs encontrados: {[f'{id.tipo}:{id.valor}' for id in fuente.get_issns()]}",
                origen=self.origen,
                bd=self.db,
            )
            self.problemas_carga.append(problema)

        elif fuente.tipo == "coleccion" and fuente.tiene_isbn():
            problema = ProblemaCarga(
                elemento=f"Fuente {tipo_contexto}",
                elemento_id=self.id_publicacion,
                descripcion=f"Fuente tipo 'colección' tiene identificadores ISBN/eISBN inapropiados. "
                f"Título: {fuente.titulo}. "
                f"ISBNs encontrados: {[f'{id.tipo}:{id.valor}' for id in fuente.get_isbns()]}",
                origen=self.origen,
                bd=self.db,
            )
            self.problemas_carga.append(problema)

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

        # Check if collection is already linked to this source
        query_check = """
            SELECT COUNT(*) as count 
            FROM prisma.p_dato_fuente 
            WHERE idFuente = %(idFuente)s AND tipo = 'coleccion' AND valor = %(idColeccion)s
        """
        params_check = {
            "idFuente": self.datos.fuente.id_fuente,
            "idColeccion": self.datos.fuente.coleccion.id_fuente,
        }

        result = self.db.ejecutarConsulta(query_check, params_check)
        if result and result[0]["count"] > 0:
            # Collection association already exists, skip insertion
            return

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

        # Normalizar identificador: quitar guiones y espacios, convertir a mayúsculas
        valor_normalizado = (
            identificador.valor.replace("-", "").replace(" ", "").upper()
        )

        # Para ISSN/eISSN e ISBN/eISBN, buscar ambos tipos ya que representan la misma fuente
        tipos_busqueda = []
        if identificador.tipo.lower() in ["issn", "eissn"]:
            tipos_busqueda = ["issn", "eissn"]
        elif identificador.tipo.lower() in ["isbn", "eisbn"]:
            tipos_busqueda = ["isbn", "eisbn"]
        else:
            tipos_busqueda = [identificador.tipo]

        for identificador_antiguo in fuente_antigua.identificadores:
            # Normalizar identificador antiguo
            valor_antiguo_normalizado = (
                identificador_antiguo.valor.replace("-", "").replace(" ", "").upper()
            )

            # Comparar valores normalizados y tipos equivalentes
            if (
                valor_normalizado == valor_antiguo_normalizado
                and identificador_antiguo.tipo.lower()
                in [t.lower() for t in tipos_busqueda]
            ):
                # El identificador YA EXISTE en los datos antiguos
                return True

        # El identificador NO EXISTE en los datos antiguos, se puede agregar
        return False

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

        # Verificar si el identificador ya existe en los datos antiguos
        existe_en_antiguos = self.buscar_identificador_fuente(
            identificador, registro, tipo=tipo
        )

        # Si buscar_identificador_fuente devuelve None (decorador @busqueda), significa que no hay datos antiguos
        # En ese caso, verificar en BD y luego insertar si no existe
        # Si devuelve True, significa que ya existe en datos antiguos, no insertar
        # Si devuelve False, significa que no existe en datos antiguos, verificar BD y luego insertar
        if existe_en_antiguos is True:
            return None

        # Verificar si ya existe en la base de datos (independientemente de datos antiguos)
        # Normalizar para búsqueda equivalente
        valor_normalizado = (
            identificador.valor.replace("-", "").replace(" ", "").upper()
        )

        # Para ISSN/eISSN e ISBN/eISBN, buscar ambos tipos
        tipos_busqueda = []
        if identificador.tipo.lower() in ["issn", "eissn"]:
            tipos_busqueda = ["issn", "eissn"]
        elif identificador.tipo.lower() in ["isbn", "eisbn"]:
            tipos_busqueda = ["isbn", "eisbn"]
        else:
            tipos_busqueda = [identificador.tipo]

        # Crear placeholders para la cláusula IN
        placeholders = ", ".join([f"%(tipo_{i})s" for i in range(len(tipos_busqueda))])

        query_verificar = f"""
            SELECT COUNT(*) as count 
            FROM prisma.p_identificador_fuente 
            WHERE idFuente = %(idFuente)s 
            AND REPLACE(REPLACE(UPPER(valor), '-', ''), ' ', '') = %(valor_normalizado)s
            AND tipo IN ({placeholders})
            AND eliminado = 0
        """

        params_verificar = {
            "idFuente": fuente.id_fuente,
            "valor_normalizado": valor_normalizado,
        }

        # Agregar tipos de búsqueda al diccionario
        for i, tipo_busq in enumerate(tipos_busqueda):
            params_verificar[f"tipo_{i}"] = tipo_busq

        self.db.ejecutarConsulta(query_verificar, params_verificar)
        resultado = self.db.consultaUna(query_verificar, params_verificar)

        if resultado and resultado["count"] > 0:
            # Ya existe en la BD, no insertar
            return None

        # No existe, insertar nuevo identificador
        query = """
                INSERT INTO prisma.p_identificador_fuente (idFuente, tipo, valor, origen)
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

    def buscar_editorial_en_bd(self, editorial: DatosCargaEditorial) -> int:
        """
        Busca una editorial en la base de datos por nombre
        Retorna el id si existe, 0 si no existe
        """
        query = """
            SELECT id 
            FROM prisma.p_editor 
            WHERE nombre = %(nombre)s
            LIMIT 1
        """
        params = {"nombre": editorial.nombre}

        self.db.ejecutarConsulta(query, params)
        id_editor = self.db.get_first_cell()

        return id_editor or 0

    def buscar_editorial_asociada_fuente(
        self, fuente_id: int, nombre_editorial: str
    ) -> int:
        """
        Busca si ya existe una editorial con el mismo nombre asociada a esta fuente
        Retorna el id si existe la asociación, 0 si no existe
        """
        query = """
            SELECT pdf.valor as id_editor
            FROM prisma.p_dato_fuente pdf
            JOIN prisma.p_editor pe ON pe.id = pdf.valor
            WHERE pdf.idFuente = %(idFuente)s 
            AND pdf.tipo = 'editorial' 
            AND pe.nombre = %(nombre)s
            LIMIT 1
        """
        params = {"idFuente": fuente_id, "nombre": nombre_editorial}

        self.db.ejecutarConsulta(query, params)
        id_editor = self.db.get_first_cell()

        return id_editor or 0

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

        # Primero: verificar si ya existe una editorial con este nombre asociada a esta fuente
        id_editor_asociado = self.buscar_editorial_asociada_fuente(
            fuente.id_fuente, editorial.nombre
        )
        if id_editor_asociado:
            # Ya existe una editorial con el mismo nombre asociada a esta fuente, no hacer nada
            return

        # Segundo: buscar si la editorial existe en la base de datos
        id_editor = self.buscar_editorial_en_bd(editorial)

        if not id_editor:
            # La editorial no existe, crearla
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

        # Tercero: verificar explícitamente si ya existe esta asociación editorial-fuente
        query_verificar = """
            SELECT COUNT(*) as count 
            FROM prisma.p_dato_fuente pdf
            JOIN prisma.p_editor pe ON pe.id = pdf.valor
            WHERE pdf.idFuente = %(idFuente)s 
            AND pdf.tipo = 'editorial' 
            AND pe.nombre = %(nombre)s
        """
        params_verificar = {"idFuente": fuente.id_fuente, "nombre": editorial.nombre}

        resultado = self.db.consultaUna(query_verificar, params_verificar)

        if resultado and resultado["count"] > 0:
            # Ya existe una editorial con este nombre asociada a esta fuente
            return

        # Cuarto: asociar la editorial con la fuente (solo si no existe)
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
        # Check if collection is already linked to this source
        query_check = """
            SELECT COUNT(*) as count 
            FROM prisma.p_dato_fuente 
            WHERE idFuente = %(idFuente)s AND tipo = 'coleccion' AND valor = %(idColeccion)s
        """
        params_check = {
            "idFuente": self.datos.fuente.id_fuente,
            "idColeccion": id_coleccion,
        }

        result = self.db.ejecutarConsulta(query_check, params_check)
        if result and result[0]["count"] > 0:
            # Collection association already exists, skip insertion
            return

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
