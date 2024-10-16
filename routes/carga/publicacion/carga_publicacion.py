from datetime import datetime

import pandas as pd
from db.conexion import BaseDatos
from routes.carga.problema_carga import ProblemaCarga
from routes.carga.publicacion.comparar_autores import (
    ComparacionAutores,
)
from routes.carga.publicacion.datos_carga_publicacion import (
    DatosCargaAutor,
    DatosCargaDatoPublicacion,
    DatosCargaIdentificadorPublicacion,
    DatosCargaIdentificadorFuente,
    DatosCargaPublicacion,
)
from routes.carga.publicacion.idus.parser import IdusParser


class CargaPublicacion:
    def __init__(self, db: BaseDatos = None, id_carga=None) -> None:
        self.id_carga = id_carga or datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]
        self.datos: DatosCargaPublicacion = None
        self.start_database(db)
        self.id_publicacion = 0
        self.duplicado = False
        self.fuente_existente = False
        self.origen = None
        self.problemas: list[ProblemaCargaPublicacion] = []

    def start_database(self, db: BaseDatos):
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
        self.db.connection.rollback()
        self.db.connection.close()

    def close_database(self):
        self.db.connection.commit()
        self.db.connection.close()

    def add_problema(
        self,
        tipo_problema: str,
        mensaje: str,
        antigua_fuente: str,
        antiguo_valor: str,
        nueva_fuente: str,
        nuevo_valor: str,
        tipo_dato_2: str = None,
        tipo_dato_3: str = None,
    ):

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

    def add_carga_idus(self, handle: str):
        parser = IdusParser(handle=handle)
        self.datos = parser.datos_carga_publicacion

    def cargar_publicacion(self):
        if self.es_duplicado():
            return self.id_publicacion

        self.insertar_publicacion()
        self.insertar_autores()
        self.insertar_identificadores_publicacion()
        self.insertar_datos_publicacion()
        self.insertar_problemas()

    def es_duplicado(self):
        identificadores = ",".join(
            identificador.valor for identificador in self.datos.identificadores
        )
        query = """
                SELECT p.idPublicacion FROM prisma.p_publicacion p
                LEFT JOIN prisma.p_identificador_publicacion idp ON idp.idPublicacion = p.idPublicacion
                WHERE idp.valor IN (%(identificadores)s)
                """
        params = {"identificadores": identificadores}

        self.db.ejecutarConsulta(query, params)
        id_publicacion = self.db.get_first_cell()

        if id_publicacion:
            self.id_publicacion = id_publicacion
            self.duplicado = True
        else:
            self.duplicado = False

        return self.duplicado

    def insertar_publicacion(self):
        query = """INSERT INTO prisma.p_publicacion (tipo, titulo, agno, origen)
                    VALUES (%(tipo)s, %(titulo)s, %(agno)s, %(origen)s)"""

        params = {
            "tipo": self.datos.tipo,
            "titulo": self.datos.titulo,
            "agno": self.datos.año_publicacion,
            "origen": self.datos.tipo,
        }

        self.db.ejecutarConsulta(query, params)
        self.id_publicacion = self.db.last_id

    def buscar_autores(self):
        query = (
            """SELECT * FROM prisma.p_autor WHERE idPublicacion = %(idPublicacion)s"""
        )
        params = {"idPublicacion": self.id_publicacion}

        self.db.ejecutarConsulta(query, params)
        antiguos_autores = self.db.get_dataframe()

        if len(antiguos_autores) == 0:
            return False
        else:
            nuevos_autores = pd.DataFrame(self.datos.to_dict().get("autores").values())
            comparacion_autores = ComparacionAutores(antiguos_autores, nuevos_autores)
            comparacion_autores.comparar()

            return True

    def insertar_autores(self):
        if self.buscar_autores():
            return None
        for autor in self.datos.autores:
            self.insertar_autor(autor)

    def insertar_autor(self, autor: DatosCargaAutor):
        query = """INSERT INTO prisma.p_autor (orden,firma, rol, idPublicacion, idInvestigador)
                    VALUES (%(orden)s, %(firma)s, %(rol)s, %(idPublicacion)s, %(idInvestigador)s)
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

    def buscar_id_investigador(self, autor: DatosCargaAutor):

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

    def insertar_identificadores_publicacion(self):
        for identificador in self.datos.identificadores:
            self.insertar_identificador_publicacion(identificador)

    def buscar_identificador_publicacion(
        self, identificador: DatosCargaIdentificadorPublicacion
    ) -> bool:
        query = """
                SELECT * FROM prisma.p_identificador_publicacion WHERE idPublicacion = %(idPublicacion)s AND tipo = %(tipo)s
                """
        params = {
            "idPublicacion": self.id_publicacion,
            "tipo": identificador.tipo,
        }
        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        if df.empty:
            return False
        if df.iloc[0]["origen"] == self.origen:
            return False
        else:
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

    def insertar_fuente(self) -> int:
        id_fuente = self.buscar_fuente()

        if not self.fuente_existente:

            query = """INSERT INTO prisma.p_fuente (tipo, titulo, editorial, origen)
                VALUES (%(tipo)s, %(titulo)s, %(editorial)s, %(origen)s)"""

            params = {
                "tipo": self.datos.fuente.tipo,
                "titulo": self.datos.fuente.titulo,
                "editorial": self.datos.fuente.editoriales[0].nombre,
                "origen": self.origen,
            }

            self.db.ejecutarConsulta(query, params)
            id_fuente = self.db.last_id

        self.datos.fuente.id_fuente = id_fuente

        for identificador in self.datos.fuente.identificadores:
            self.insertar_identificador_fuente(identificador, id_fuente)

        return id_fuente

    def insertar_identificador_fuente(
        self, identificador: DatosCargaIdentificadorFuente, id_fuente: int
    ):
        query = """
                INSERT INTO prisma.p_identificador_fuente (idFuente, tipo, valor, origen)
                VALUES (%(idFuente)s, %(tipo)s, %(valor)s, %(origen)s)
                """
        params = {
            "idFuente": id_fuente,
            "tipo": identificador.tipo,
            "valor": identificador.valor,
            "origen": self.origen,
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
