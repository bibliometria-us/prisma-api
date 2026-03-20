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
        bd: BaseDatos,
    ):
        self.bd = bd or BaseDatos()
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
        query = (
            "SELECT id FROM prisma_proyectos.proyecto WHERE sisius_id = %(sisius_id)s"
        )
        params = {"sisius_id": sisius_id}

        self.bd.ejecutarConsulta(query, params=params)

        proyecto_id = self.bd.get_first_cell()

        if not proyecto_id:
            return None

        return proyecto_id

    def id_investigador_por_dni(self) -> int:

        query = (
            "SELECT idInvestigador FROM prisma.i_investigador WHERE docuIden=%(dni)s"
        )
        params = {"dni": self.dni}

        self.bd.ejecutarConsulta(query, params=params)

        id_investigador = self.bd.get_first_cell()

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
        query = """
        SELECT id FROM prisma_proyectos.proyecto_miembro 
        WHERE
        proyecto_id = %(proyecto_id)s AND firma = %(firma)s AND rol = %(rol)s
        """
        params = {
            "proyecto_id": self.proyecto_id,
            "firma": self.firma,
            "rol": self.rol,
        }

        self.bd.ejecutarConsulta(query, params=params)
        self.id_miembro = self.bd.get_first_cell()

    def cargar(self) -> list[str]:
        self.buscar_contrato_existente()

        # Si el proyecto para este contrato no está cargado, no se hace nada
        if not self.proyecto_id:
            return []

        # Si no existe este contrato, se da de alta y se devuelve el mensaje de alta
        if not self.id_miembro and not self.ha_expirado:
            return [self.alta_miembro()]

        # Si no existe el contrato pero ya está expirado, se da de alta pero no se devuelve mensaje.
        if not self.id_miembro and self.ha_expirado:
            self.alta_miembro()
            return []
        return []

    def baja_miembro(self) -> str:
        query = (
            "DELETE FROM prisma_proyectos.proyecto_miembro WHERE id = %(id_miembro)s"
        )
        params = {"id_miembro": self.id_miembro}

        self.bd.ejecutarConsulta(query, params=params)

        if self.bd.error:
            return f"Error dando de baja el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

        if self.bd.rowcount == 1:
            return f"Dado de baja el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

    def alta_miembro(self) -> str:
        query = (
            "INSERT INTO prisma_proyectos.proyecto_miembro (proyecto_id, firma, rol, investigador_id, fecha_alta, fecha_baja) "
            "VALUES (%(proyecto_id)s, %(firma)s, %(rol)s, %(investigador_id)s, %(fecha_alta)s, %(fecha_baja)s)"
        )

        params = {
            "proyecto_id": self.proyecto_id,
            "firma": self.firma,
            "rol": self.rol,
            "investigador_id": self.investigador_id,
            "fecha_alta": self.fecha_inicio,
            "fecha_baja": self.fecha_fin or self.fecha_renuncia,
        }

        self.bd.ejecutarConsulta(query, params=params)

        if self.bd.error:
            return f"Error dando de alta el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"

        if self.bd.rowcount == 1:
            return f"Dado de alta el contrato del investigador {self.firma} en el proyecto con id {self.proyecto_id}"
