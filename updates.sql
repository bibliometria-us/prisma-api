# QUERIES PARA CENTROS CENSADOS

ALTER TABLE prisma.i_investigador ADD idCentroCenso varchar(5) DEFAULT NULL NULL;
ALTER TABLE prisma.i_investigador CHANGE idCentroCenso idCentroCenso varchar(5) DEFAULT NULL NULL AFTER idCentro;
CREATE INDEX i_investigador_idCentroCenso_IDX USING BTREE ON prisma.i_investigador (idCentroCenso);

CREATE OR REPLACE
ALGORITHM = UNDEFINED VIEW `prisma`.`i_investigador_activo` AS
select
    `prisma`.`i_investigador`.`idInvestigador` AS `idInvestigador`,
    `prisma`.`i_investigador`.`nombre` AS `nombre`,
    `prisma`.`i_investigador`.`apellidos` AS `apellidos`,
    `prisma`.`i_investigador`.`docuIden` AS `docuIden`,
    `prisma`.`i_investigador`.`email` AS `email`,
    `prisma`.`i_investigador`.`idCategoria` AS `idCategoria`,
    `prisma`.`i_investigador`.`fechaNombramiento` AS `fechaNombramiento`,
    `prisma`.`i_investigador`.`idArea` AS `idArea`,
    `prisma`.`i_investigador`.`fechaContratacion` AS `fechaContratacion`,
    `prisma`.`i_investigador`.`idDepartamento` AS `idDepartamento`,
    `prisma`.`i_investigador`.`idCentro` AS `idCentro`,
    `prisma`.`i_investigador`.`idCentroCenso` AS `idCentroCenso`,
    `prisma`.`i_investigador`.`sexo` AS `sexo`,
    `prisma`.`i_investigador`.`resumen` AS `resumen`,
    `prisma`.`i_investigador`.`nacionalidad` AS `nacionalidad`,
    `prisma`.`i_investigador`.`fechaNacimiento` AS `fechaNacimiento`,
    `prisma`.`i_investigador`.`perfilPublico` AS `perfilPublico`,
    `prisma`.`i_investigador`.`fechaActualizacion` AS `fechaActualizacion`
from
    `prisma`.`i_investigador`
where
    `prisma`.`i_investigador`.`idInvestigador` in (
    select
        distinct `prisma`.`i_fecha_cese`.`idInvestigador`
    from
        `prisma`.`i_fecha_cese`) is false WITH CASCADED CHECK OPTION;
