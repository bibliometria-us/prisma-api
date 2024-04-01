from models.attribute import Attribute
from models.grupo import Grupo
from models.model import Component, Model
from models.colectivo.unidad_excelencia import UnidadExcelencia
from models.colectivo.centro_mixto import CentroMixto
from models.colectivo.instituto import Instituto


class Investigador(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_investigador",
        alias="investigador",
        primary_key="idInvestigador",
        grupo=Grupo(),
        unidad_excelencia=UnidadExcelencia(),
        centro_mixto=CentroMixto(),
        instituto=Instituto(),
    ):
        attributes = [
            Attribute(column_name="idInvestigador"),
            Attribute(column_name="nombre"),
            Attribute(column_name="apellidos"),
            Attribute(column_name="docuIden", visible=False),
            Attribute(column_name="email"),
            Attribute(column_name="idCategoria"),
            Attribute(column_name="idArea"),
            Attribute(column_name="fechaContratacion"),
            Attribute(column_name="idDepartamento"),
            Attribute(column_name="idCentro"),
            Attribute(column_name="nacionalidad"),
            Attribute(column_name="sexo"),
            Attribute(column_name="fechaNacimiento"),
            Attribute(column_name="fechaNombramiento"),
            Attribute(column_name="perfilPublico"),
        ]
        components = [
            Component(
                Grupo,
                "grupo",
                "get_grupo",
                enabled=True,
            ),
            Component(
                UnidadExcelencia,
                "unidad_excelencia",
                "get_unidad_excelencia",
                enabled=True,
            ),
            Component(
                CentroMixto,
                "centro_mixto",
                "get_centro_mixto",
                enabled=True,
            ),
            Component(
                Instituto,
                "instituto",
                "get_instituto",
                enabled=True,
            ),
        ]
        self.grupo = grupo
        self.unidad_excelencia = unidad_excelencia
        self.centro_mixto = centro_mixto
        self.instituto = instituto

        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
        )

    # GRUPOS

    def get_grupo(self) -> Grupo:
        query = "SELECT MAX(idGrupo) FROM prisma.i_grupo_investigador WHERE idInvestigador = %(idInvestigador)s"

        params = {"idInvestigador": self.get_attribute_value("idInvestigador")}

        result = self.db.ejecutarConsulta(query, params)
        id_grupo = result[1][0]

        if id_grupo:
            self.grupo.set_attribute("idGrupo", id_grupo)
            self.grupo.get()

        return self.grupo

    def actualizar_grupo(self, id_grupo, rol) -> None:
        query = """REPLACE INTO prisma.i_grupo_investigador (idInvestigador, idGrupo, rol)
        VALUES (%(idInvestigador)s, %(idGrupo)s, %(rol)s)"""

        params = {
            "idInvestigador": self.get_attribute_value("idInvestigador"),
            "idGrupo": id_grupo,
            "rol": rol,
        }

        result = self.db.ejecutarConsulta(query, params)

        return None

    def eliminar_grupo(self) -> None:
        self.actualizar_grupo("0", "Miembro")

    # UNIDADES DE EXCELENCIA

    def get_unidad_excelencia(self) -> UnidadExcelencia:
        self.unidad_excelencia.get_colectivo_from_investigador(
            self.get_primary_key().value
        )

        return self.unidad_excelencia

    def update_unidad_excelencia(
        self, unidad_excelencia, rol, actualizado=True
    ) -> None:
        self.unidad_excelencia.set_attribute("idUdExcelencia", unidad_excelencia)
        self.unidad_excelencia.get()
        self.unidad_excelencia.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_unidad_excelencia(self) -> None:
        self.unidad_excelencia.delete_colectivo_from_investigador(
            self.get_primary_key().value
        )
        self.unidad_excelencia = UnidadExcelencia()

    # CENTROS MIXTOS

    def get_centro_mixto(self) -> None:
        self.centro_mixto.get_colectivo_from_investigador(self.get_primary_key().value)

        return self.centro_mixto

    def update_centro_mixto(self, centro_mixto, rol, actualizado=True) -> None:
        self.centro_mixto.set_attribute("idCentroMixto", centro_mixto)
        self.centro_mixto.get()
        self.centro_mixto.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_centro_mixto(self) -> None:
        self.centro_mixto.delete_colectivo_from_investigador(
            self.get_primary_key().value
        )
        self.centro_mixto = CentroMixto()

    # INSTITUTOS

    def get_instituto(self) -> None:
        self.instituto.get_colectivo_from_investigador(self.get_primary_key().value)

        return self.instituto

    def update_instituto(self, instituto, rol, actualizado=True) -> None:
        self.instituto.set_attribute("idInstituto", instituto)
        self.instituto.get()
        self.instituto.update_colectivo_from_investigador(
            idInvestigador=self.get_primary_key().value,
            rol=rol,
            actualizado=actualizado,
        )

    def delete_instituto(self) -> None:
        self.instituto.delete_colectivo_from_investigador(self.get_primary_key().value)
        self.instituto = Instituto()
