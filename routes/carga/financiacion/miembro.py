from pandas import DataFrame, Timestamp
import pandas as pd
from db.conexion import BaseDatos


class Miembro:
    def __init__(
        self,
        sisius_id: int,
        nombre: str,
        apellidos: str,
        rol: str,
        dni: int,
        fecha_inicio: Timestamp,
        fecha_fin: Timestamp,
        fecha_renuncia: Timestamp,
    ):
        self.nombre = nombre
        self.apellidos = apellidos
        self.firma = self.crear_firma()
        self.rol = rol
        self.dni = dni
        self.investigador_id = self.id_investigador_por_dni()
        self.proyecto_id = self.buscar_proyecto_id(sisius_id)
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.fecha_renuncia = fecha_renuncia
        self.id_miembro: int = None
        self.ha_expirado = self.comprobar_expiracion()

    def buscar_proyecto_id(self, sisius_id: int) -> int:
        db = BaseDatos(database="prisma_proyectos")
        query = "SELECT id FROM proyecto WHERE sisius_id = %(sisius_id)s"
        params = {"sisius_id": sisius_id}

        db.ejecutarConsulta(query, params=params)

        proyecto_id = db.get_first_cell()

        if not proyecto_id:
            return None

        return proyecto_id

    def id_investigador_por_dni(self) -> int:
        db = BaseDatos()

        query = "SELECT idInvestigador FROM i_investigador WHERE docuIden=%(dni)s"
        params = {"dni": self.dni}

        db.ejecutarConsulta(query, params=params)

        id_investigador = db.get_first_cell()

        if not id_investigador:
            return None

        return id_investigador

    def crear_firma(self) -> str:
        return f"{self.apellidos}, {self.nombre}"

    def comprobar_expiracion(self) -> bool:
        now = pd.Timestamp.now().date()

        if self.fecha_inicio and self.fecha_inicio > now:
            return True
        if self.fecha_fin and self.fecha_fin <= now:
            return True
        if self.fecha_renuncia and self.fecha_renuncia <= now:
            return True

        return False

    def buscar_contrato_existente(self) -> None:
        db = BaseDatos(database="prisma_proyectos")
        query = """
        SELECT id FROM proyecto_miembro 
        WHERE
        proyecto_id = %(proyecto_id)s AND firma = %(firma)s AND rol = %(rol)s
        """
        params = {
            "proyecto_id": self.proyecto_id,
            "firma": self.firma,
            "rol": self.rol,
        }

        db.ejecutarConsulta(query, params=params)
        self.id_miembro = db.get_first_cell()

    def cargar(self) -> list[str]:
        self.buscar_contrato_existente()

        # Si el proyecto para este contrato no está cargado, no se hace nada
        if not self.proyecto_id:
            return []

        # Si ya existe el contrato pero ha expirado, se da de baja
        if self.id_miembro and self.ha_expirado:
            return [self.baja_miembro()]

        # Si no existe este contrato y tampoco ha expirado, se da de alta
        if not self.id_miembro and not self.ha_expirado:
            return [self.alta_miembro()]

        return []

    def baja_miembro(self) -> str:
        db = BaseDatos("prisma_proyectos")
        query = "DELETE FROM proyecto_miembro WHERE id = %(id_miembro)s"
        params = {"id_miembro": self.id_miembro}

        db.ejecutarConsulta(query, params=params)

        if db.error:
            return f"Error dando de baja el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

        if db.rowcount == 1:
            return f"Dado de baja el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

    def alta_miembro(self) -> str:
        db = BaseDatos("prisma_proyectos")
        query = (
            "INSERT INTO proyecto_miembro (proyecto_id, firma, rol, investigador_id)"
            "VALUES (%(proyecto_id)s, %(firma)s, %(rol)s, %(investigador_id)s)"
        )

        params = {
            "proyecto_id": self.proyecto_id,
            "firma": self.firma,
            "rol": self.rol,
            "investigador_id": self.investigador_id,
        }

        db.ejecutarConsulta(query, params=params)

        if db.error:
            return f"Error dando de alta el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

        if db.rowcount == 1:
            return f"Dado de alta el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"
