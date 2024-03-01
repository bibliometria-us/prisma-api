from models.attribute import Attribute
from models.grupo import Grupo
from models.model import Component, Model


class Investigador(Model):

    def __init__(
        self,
        db_name="prisma",
        table_name="i_investigador",
        alias="investigador",
        primary_key="idInvestigador",
        grupo=Grupo(),
    ):
        attributes = [
            Attribute(column_name="idInvestigador"),
            Attribute(column_name="nombre"),
            Attribute(column_name="apellidos"),
            Attribute(column_name="docuIden"),
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
            )
        ]
        self.grupo = grupo
        super().__init__(
            db_name,
            table_name,
            alias,
            primary_key,
            attributes=attributes,
            components=components,
        )

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