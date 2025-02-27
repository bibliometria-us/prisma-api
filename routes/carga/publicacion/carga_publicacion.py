from datetime import datetime
from typing import Any, Dict

import pandas as pd
from db.conexion import BaseDatos
from routes.carga.problema_carga import ProblemaCarga
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


class CargaPublicacion:
    """
    Clase que representa una carga de publicación de publicación generica
    """

    def __init__(self, db: BaseDatos = None, id_carga=None, auto_commit=True) -> None:
        self.id_carga = id_carga or datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]
        self.datos: DatosCargaPublicacion = None
        self.datos_antiguos: DatosCargaPublicacion = None
        self.auto_commit = auto_commit
        self.start_database(db)
        self.id_publicacion = 0
        self.fuente_existente = False
        self.origen = None
        self.problemas: list[ProblemaCargaPublicacion] = []

    def busqueda(func):
        def wrapper(self, *args, **kwargs):
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

    def close_database(self):
        """
        Crea la cierra y persiste cambios en la conexión con la base de datos
        """
        if self.auto_commit:
            self.db.commit()
            self.db.closeConnection()

    def add_problema(
        self,
        mensaje: str,
        antigua_fuente: str,
        antiguo_valor: str,
        nueva_fuente: str,
        nuevo_valor: str,
        tipo_problema: str = "Advertencia",
        tipo_dato_2: str = None,
        tipo_dato_3: str = None,
    ):
        """
        Añade un elemento que representa un problema en la carga de publicaciones.
        """

        problema = ProblemaCargaPublicacion(
            id_carga=self.id_carga,
            db=self.db,
            id_dato=self.id_publicacion,
            antigua_fuente=antigua_fuente,
            antiguo_valor=antiguo_valor,
            nueva_fuente=nueva_fuente,
            nuevo_valor=nuevo_valor,
            tipo_dato_2=tipo_dato_2,
            tipo_dato_3=tipo_dato_3,
        )

        problema.tipo_dato = "Publicacion"
        if tipo_problema == "Advertencia":
            problema.advertencia(mensaje)
        if tipo_problema == "Error":
            problema.error(mensaje)

        self.problemas.append(problema)

    def insertar_problemas(self):
        for problema in self.problemas:
            problema.insertar()

    def cargar_publicacion(self):
        self.insertar_publicacion()
        self.insertar_autores()
        self.insertar_identificadores_publicacion()
        self.insertar_datos_publicacion()
        self.insertar_fuente()
        self.insertar_problemas()
        self.insertar_financiaciones()
        self.insertar_fechas_publicacion()
        self.insertar_valores_acceso_abierto()
        self.close_database()

    def buscar_publicacion(self):
        """
        Comprueba si una publicación está duplicada según criterios establecidos
        """
        self.datos_antiguos = DatosCargaPublicacion().from_id_publicacion(
            self.id_publicacion, self.db
        )

        identificadores = ",".join(
            f"'{identificador.valor}'" for identificador in self.datos.identificadores
        )
        # DUDA: Se comprueba sólo el id y no el tipo de id (o fuente)
        # Se buscan las publicaciones por el o los identificadores (DOI, ID Scopus, ID WoS...) localizados en la carga
        query = f"""
                SELECT p.idPublicacion, p.tipo, p.titulo, p.agno as año_publicacion, p.origen FROM prisma.p_publicacion p
                LEFT JOIN prisma.p_identificador_publicacion idp ON idp.idPublicacion = p.idPublicacion
                WHERE idp.valor IN ({identificadores})
                GROUP BY p.idPublicacion
                """
        # params = {"identificadores": identificadores}

        self.db.ejecutarConsulta(query)
        id_publicacion = self.db.get_first_cell()

        datos_publicacion = None

        if id_publicacion:
            self.id_publicacion = id_publicacion
            datos_publicacion = self.db.get_dataframe()
            datos_publicacion = datos_publicacion.iloc[0].to_dict()
            self.datos_antiguos = DatosCargaPublicacion().from_id_publicacion(
                id_publicacion, self.db
            )

        return datos_publicacion

    def comparar_publicacion(self, publicacion_antigua: Dict[str, Any]):
        """
        Si se ha encontrado una publicación existente con identificadores comunes,
        comparar los atributos entre sí.
        """

        tipos_atributo = ["tipo", "titulo", "año_publicacion"]  # Columnas a comprobar

        for tipo_atributo in tipos_atributo:
            antiguo_atributo = publicacion_antigua[tipo_atributo]
            nuevo_atributo = getattr(self.datos, tipo_atributo)
            if not antiguo_atributo == nuevo_atributo:
                self.add_problema(
                    mensaje="",
                    antigua_fuente=publicacion_antigua["origen"],
                    antiguo_valor=antiguo_atributo,
                    nueva_fuente=self.origen,
                    nuevo_valor=nuevo_atributo,
                    tipo_dato_2=tipo_atributo,
                )

    def insertar_publicacion(self):
        """
        Inserta la publicación en base de dato
        DUDA: esto es lo único que se almacena de la publicación en Prisma?
        """
        publicacion_antigua = self.buscar_publicacion()
        if publicacion_antigua:
            self.comparar_publicacion(publicacion_antigua=publicacion_antigua)
            return None

        query = """INSERT INTO prisma.p_publicacion (tipo, titulo, agno, origen)
                    VALUES (%(tipo)s, %(titulo)s, %(agno)s, %(origen)s)"""

        params = {
            "tipo": self.datos.tipo,
            "titulo": self.datos.titulo,
            "agno": self.datos.año_publicacion,
            "origen": self.origen,
        }

        self.db.ejecutarConsulta(query, params)
        self.id_publicacion = self.db.last_id

    def buscar_autores(self):
        """
        Busca los autores en la base de datos para asignarlos a la publicación
        """
        query = """SELECT *, rol as tipo FROM prisma.p_autor WHERE idPublicacion = %(idPublicacion)s"""
        params = {"idPublicacion": self.id_publicacion}

        self.db.ejecutarConsulta(query, params)
        antiguos_autores = self.db.get_dataframe()

        if len(antiguos_autores) == 0:
            return False
        else:
            nuevos_autores = pd.DataFrame(self.datos.to_dict().get("autores"))
            comparacion_autores = ComparacionAutores(antiguos_autores, nuevos_autores)
            count_nuevos, count_antiguos = comparacion_autores.comparar(
                tipo_comparacion="cantidad"
            )

            if count_nuevos != count_antiguos:
                self.add_problema(
                    mensaje="",
                    antigua_fuente=None,
                    antiguo_valor=str(count_antiguos),
                    nueva_fuente=self.origen,
                    nuevo_valor=str(count_nuevos),
                    tipo_dato_2="autores",
                )

            return True

    def insertar_autores(self):
        """
        Inserta los autores de una publicación.
        """
        if self.buscar_autores():
            return None
        for autor in self.datos.autores:
            self.insertar_autor(autor)

    def buscar_id_investigador(self, autor: DatosCargaAutor):
        # TODO: Plantear que la id de investigador de autor se actualice siempre
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

    def buscar_identificador_publicacion(
        self, identificador: DatosCargaIdentificadorPublicacion
    ) -> bool:
        query = """
                SELECT valor, origen FROM prisma.p_identificador_publicacion WHERE idPublicacion = %(idPublicacion)s AND tipo = %(tipo)s
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": identificador.tipo,
        }
        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        if df.empty:
            return False
        if df.iloc[0]["valor"] == identificador.valor:
            # Si el valor es el mismo, se devuelve duplicado a True para no insertarlo
            return True
        else:
            # Si el valor no es el mismo, se devuelve duplicado a True para no insertarlo, y se introduce problema
            mensaje = f"Publicación {self.id_publicacion}. Identificador '{identificador.tipo}'. Actual ({df.iloc[0]['origen']}): {df.iloc[0]['valor']}, Nuevo: ({self.origen}): {identificador.valor}"
            kwargs = {
                "tipo_problema": "Advertencia",
                "mensaje": mensaje,
                "antigua_fuente": df.iloc[0]["origen"],
                "antiguo_valor": df.iloc[0]["valor"],
                "nueva_fuente": self.origen,
                "nuevo_valor": identificador.valor,
                "tipo_dato_2": "Identificador",
                "tipo_dato_3": identificador.tipo,
            }
            self.add_problema(**kwargs)
            return True

    def insertar_identificador_publicacion(
        self, identificador: DatosCargaIdentificadorPublicacion
    ):
        if self.buscar_identificador_publicacion(identificador):
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

    def insertar_datos_publicacion(self):
        for dato in self.datos.datos:
            self.insertar_dato_publicacion(dato)

    def buscar_dato_publicacion(self, dato: DatosCargaDatoPublicacion) -> bool:
        query = """
                SELECT * FROM prisma.p_dato_publicacion WHERE idPublicacion = %(idPublicacion)s AND tipo = %(tipo)s
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": dato.tipo,
        }
        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        if df.empty:
            return False
        else:
            mensaje = f"Publicación {self.id_publicacion}. Dato '{dato.tipo}'. Actual ({df.iloc[0]['origen']}): {df.iloc[0]['valor']}, Nuevo: ({self.origen}): {dato.valor}"
            kwargs = {
                "tipo_problema": "Advertencia",
                "mensaje": mensaje,
                "antigua_fuente": df.iloc[0]["origen"],
                "antiguo_valor": df.iloc[0]["valor"],
                "nueva_fuente": self.origen,
                "nuevo_valor": dato.valor,
                "tipo_dato_2": "Dato",
                "tipo_dato_3": dato.tipo,
            }
            self.add_problema(**kwargs)
            return True

    def insertar_dato_publicacion(self, dato: DatosCargaDatoPublicacion):
        if self.buscar_dato_publicacion(dato):
            return None

        query = """
                INSERT INTO prisma.p_dato_publicacion (idPublicacion, tipo, valor, origen)
                VALUES (%(idPublicacion)s, %(tipo)s, %(valor)s, %(origen)s)
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": dato.tipo,
            "valor": dato.valor,
            "origen": self.origen,
        }

        self.db.ejecutarConsulta(query, params)

    def buscar_fuente(self) -> int:
        if not self.datos.fuente.identificadores:
            return None

        where = " OR ".join(
            f"(idf.tipo = '{identificador.tipo}' AND idf.valor = '{identificador.valor}')"
            for identificador in self.datos.fuente.identificadores
        )

        query = f"""
                SELECT f.idFuente FROM prisma.p_fuente f
                LEFT JOIN prisma.p_identificador_fuente idf ON idf.idFuente = f.idFuente
                WHERE {where}
                """

        self.db.ejecutarConsulta(query)
        id_fuente = self.db.get_first_cell()

        if id_fuente:
            self.fuente_existente = True
            return id_fuente
        else:
            return self.db.last_id

    def comparar_fuente(self):
        pass

    def insertar_fuente(self) -> int:
        id_fuente = self.buscar_fuente()
        if self.fuente_existente:
            self.comparar_fuente()
        else:

            query = """INSERT INTO prisma.p_fuente (tipo, titulo, editorial, origen)
                VALUES (%(tipo)s, %(titulo)s, %(editorial)s, %(origen)s)"""

            editorial = (
                self.datos.fuente.editoriales[0].nombre
                if self.datos.fuente.editoriales
                else None
            )

            params = {
                "tipo": self.datos.fuente.tipo,
                "titulo": self.datos.fuente.titulo,
                "editorial": editorial,
                "origen": self.origen,
            }

            self.db.ejecutarConsulta(query, params)
            id_fuente = self.db.last_id

        self.datos.fuente.id_fuente = id_fuente

        self.insertar_id_fuente_publicacion()
        self.insertar_datos_fuente()
        self.insertar_identificadores_fuente()
        self.insertar_editoriales_fuente()

        return id_fuente

    def insertar_id_fuente_publicacion(self):
        query_insertar_id_fuente_publicacion = "UPDATE prisma.p_publicacion SET idFuente = %(idFuente)s WHERE idPublicacion = %(idPublicacion)s"
        params_insertar_id_fuente_publicacion = {
            "idFuente": self.datos.fuente.id_fuente,
            "idPublicacion": self.id_publicacion,
        }

        self.db.ejecutarConsulta(
            query_insertar_id_fuente_publicacion, params_insertar_id_fuente_publicacion
        )

    def insertar_identificadores_fuente(self):
        for identificador in self.datos.fuente.identificadores:
            self.insertar_identificador_fuente(identificador)

    def insertar_identificador_fuente(
        self, identificador: DatosCargaIdentificadorFuente
    ):
        query = """
                REPLACE INTO prisma.p_identificador_fuente (idFuente, tipo, valor, origen)
                VALUES (%(idFuente)s, %(tipo)s, %(valor)s, %(origen)s)
                """
        params = {
            "idFuente": self.datos.fuente.id_fuente,
            "tipo": identificador.tipo,
            "valor": identificador.valor,
            "origen": self.origen,
        }

        self.db.ejecutarConsulta(query, params)

    def insertar_datos_fuente(self):
        for dato in self.datos.fuente.datos:
            self.insertar_dato_fuente(dato)

    def insertar_dato_fuente(self, dato: DatosCargaDatosFuente):
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

    def insertar_editoriales_fuente(self):
        for editorial in self.datos.fuente.editoriales:
            self.insertar_editorial_fuente(editorial)

    def insertar_editorial_fuente(self, editorial: DatosCargaEditorial):
        editorial_antigua = self.buscar_editorial(editorial)

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

        else:
            id_editor = editorial_antigua.id_editor

        query_insertar_editorial_fuente = """ INSERT INTO prisma.p_dato_fuente (idFuente, tipo, valor) VALUES (%(idFuente)s, 'editorial', %(idEditor)s)"""
        params_insertar_editorial_fuente = {
            "idFuente": self.datos.fuente.id_fuente,
            "idEditor": id_editor,
        }

        self.db.ejecutarConsulta(
            query_insertar_editorial_fuente, params_insertar_editorial_fuente
        )

    @busqueda
    def buscar_editorial(self, editorial: DatosCargaEditorial) -> DatosCargaEditorial:
        if not self.datos_antiguos:
            return None

        for editorial_antigua in self.datos_antiguos.fuente.editoriales:
            if editorial_antigua.nombre == editorial.nombre:
                return editorial_antigua

        return None

    def insertar_fechas_publicacion(self):
        for fecha in self.datos.fechas_publicacion:
            self.insertar_fecha_publicacion(fecha)

    @busqueda
    def buscar_fecha_publicacion(self, fecha: DatosCargaFechaPublicacion):
        if fecha in self.datos_antiguos.fechas_publicacion:
            return True

        return False

    def insertar_fecha_publicacion(self, fecha: DatosCargaFechaPublicacion):
        if self.buscar_fecha_publicacion(fecha):
            return None

        query = """
                INSERT INTO prisma.p_fecha_publicacion (idPublicacion, tipo, mes, agno)
                VALUES (%(idPublicacion)s, %(tipo)s, %(mes)s, %(agno)s)
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": fecha.tipo,
            "mes": fecha.mes,
            "agno": fecha.agno,
        }

        self.db.ejecutarConsulta(query, params)

    def insertar_financiaciones(self):
        for financiacion in self.datos.financiacion:
            self.insertar_financiacion(financiacion)

    @busqueda
    def buscar_financiacion(self, financiacion):
        if financiacion in self.datos_antiguos.financiacion:
            return True

        return False

    def insertar_financiacion(self, financiacion: DatosCargaFinanciacion):
        if self.buscar_financiacion(financiacion):
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

    def insertar_valores_acceso_abierto(self):
        for acceso_abierto in self.datos.acceso_abierto:
            self.insertar_acceso_abierto(acceso_abierto)

    @busqueda
    def buscar_acceso_abierto(self, acceso_abierto: DatosCargaAccesoAbierto):
        if acceso_abierto in self.datos_antiguos.acceso_abierto:
            return True

        return False

    def insertar_acceso_abierto(self, acceso_abierto: DatosCargaAccesoAbierto):
        if self.buscar_acceso_abierto(acceso_abierto):
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


class ProblemaCargaPublicacion(ProblemaCarga):
    def __init__(
        self,
        id_carga,
        db,
        id_dato,
        antigua_fuente,
        antiguo_valor,
        nueva_fuente,
        nuevo_valor,
        tipo_dato_2=None,
        tipo_dato_3=None,
    ):
        super().__init__(
            id_carga,
            db,
            id_dato,
            antigua_fuente,
            antiguo_valor,
            nueva_fuente,
            nuevo_valor,
            tipo_dato_2,
            tipo_dato_3,
        )
        self.tipo_dato = "Publicación"
