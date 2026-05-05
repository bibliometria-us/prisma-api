from db.conexion import BaseDatos
from routes.carga.investigador.datos_carga_investigador import (
    DatosCargaContratoInvestigador,
    DatosCargaInvestigador,
)


class BuscarInvestigador:
    def __init__(self, db: BaseDatos = None):
        self.db = db or BaseDatos()
        self.datos: DatosCargaInvestigador = None

    def buscar_investigador(self, documento_identidad: str) -> DatosCargaInvestigador:

        id_investigador = self.obtener_id_investigador(documento_identidad)

        if id_investigador is None:
            return None

        self.datos = DatosCargaInvestigador()
        self.datos.id = id_investigador
        self.obtener_atributos_investigador()
        self.buscar_contratos_investigador()
        self.buscar_ceses_investigador()

        return self.datos

    def obtener_id_investigador(self, documento_identidad: str) -> int:
        query = """
            SELECT idInvestigador
            FROM i_investigador
            WHERE docuIden = %(documento_identidad)s
        """
        params = {"documento_identidad": documento_identidad}

        self.db.ejecutarConsulta(query, params)

        return self.db.get_first_cell()

    def obtener_atributos_investigador(self):

        query = """
            SELECT nombre, apellidos, email, docuIden, nacionalidad, sexo, 
            fechaNacimiento
            FROM i_investigador
            WHERE idInvestigador = %(id_investigador)s
        """
        params = {"id_investigador": self.datos.id}

        self.db.ejecutarConsulta(query, params)

        df = self.db.get_dataframe().iloc[0]

        self.datos.nombre = df["nombre"]
        self.datos.apellidos = df["apellidos"]
        self.datos.email = df["email"]
        self.datos.documento_identidad = df["docuIden"]
        self.datos.nacionalidad = df["nacionalidad"]
        self.datos.sexo = int(df["sexo"] or 3)
        self.datos.fecha_nacimiento = df["fechaNacimiento"]

    def buscar_contratos_investigador(self):
        query = """
            SELECT ii.fechaContratacion, ii.fechaNombramiento, ia.idArea, id.idDepartamento, ica.idCategoria,
            ic.idCentro, ia.nombre AS nombreArea, id.nombre AS nombreDepartamento, ic.nombre AS nombreCentro, ica.nombre AS nombreCategoria
            FROM i_investigador ii
            LEFT JOIN i_area ia ON ii.idArea = ia.idArea
            LEFT JOIN i_departamento id ON ii.idDepartamento = id.idDepartamento
            LEFT JOIN i_centro ic ON ii.idCentro = ic.idCentro
            LEFT JOIN i_categoria ica ON ii.idCategoria = ica.idCategoria
            WHERE idInvestigador = %(id_investigador)s
        """
        params = {"id_investigador": self.datos.id}

        self.db.ejecutarConsulta(query, params)

        df = self.db.get_dataframe()

        if df.empty:
            return

        df = df.iloc[0]

        contrato = DatosCargaContratoInvestigador()
        contrato.fecha_contratacion = df["fechaContratacion"]
        contrato.fecha_nombramiento = df["fechaNombramiento"]
        contrato.area.id = int(df["idArea"])
        contrato.area.nombre = df["nombreArea"]
        contrato.departamento.id = df["idDepartamento"]
        contrato.departamento.nombre = df["nombreDepartamento"]
        contrato.centro.id = df["idCentro"]
        contrato.centro.nombre = df["nombreCentro"]
        contrato.categoria.id = df["idCategoria"]
        contrato.categoria.nombre = df["nombreCategoria"]

        self.datos.contratos.append(contrato)

    def buscar_ceses_investigador(self):
        for contrato in self.datos.contratos:
            self.buscar_cese_contrato(contrato)

    def buscar_cese_contrato(self, contrato: DatosCargaContratoInvestigador):
        query = """
            SELECT ifc.fechaCese, ifc.idMotivo, imc.nombre
            FROM i_fecha_cese ifc
            LEFT JOIN i_motivo_cese imc ON ifc.idMotivo = imc.idMotivo
            WHERE ifc.idInvestigador = %(id_investigador)s
            AND ifc.fechaCese >= %(fecha_contratacion)s
                """
        params = {
            "id_investigador": self.datos.id,
            "fecha_contratacion": contrato.fecha_contratacion,
        }

        self.db.ejecutarConsulta(query, params)
        df = self.db.get_dataframe()

        if df.empty:
            return

        df = df.iloc[0]
        contrato.cese.fecha = df["fechaCese"]
        contrato.cese.tipo = df["idMotivo"]
        contrato.cese.valor = df["nombre"]
