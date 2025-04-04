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
ALTER TABLE `p_dato_publicacion` ADD `origen` VARCHAR(100) NULL DEFAULT NULL AFTER `valor`; 

CREATE TABLE `a_problemas` (
  `idCarga` varchar(40) NOT NULL,
  `tipo_problema` varchar(100) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `id_dato` varchar(100) NOT NULL,
  `mensaje` varchar(100) NOT NULL DEFAULT '',
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `antigua_fuente` varchar(100) DEFAULT NULL,
  `antiguo_valor` varchar(100) NOT NULL,
  `nueva_fuente` varchar(100) NOT NULL,
  `nuevo_valor` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `a_problemas`
--
ALTER TABLE `a_problemas`
  ADD INDEX (`idCarga`);
COMMIT;

ALTER TABLE `a_problemas` CHANGE `mensaje` `mensaje` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL; 
ALTER TABLE prisma.a_problemas MODIFY COLUMN antiguo_valor text CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL;
ALTER TABLE prisma.a_problemas MODIFY COLUMN nuevo_valor text CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL;


ALTER TABLE prisma.p_dato_publicacion ADD origen varchar(100) NULL;


-- Tablas de registro y problemas

CREATE TABLE prisma.a_registro_cambios_publicacion (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NULL,
	valor TEXT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE INDEX a_registro_cambios_publicacion_id_IDX USING BTREE ON prisma.a_registro_cambios_publicacion (id,tipo_dato,tipo_dato_2,tipo_dato_3);

CREATE TABLE prisma.a_registro_problemas_publicacion (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NOT NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NOT NULL,
	valor TEXT NOT NULL,
  origen_antiguo varchar(100) NOT NULL,
	valor_antiguo TEXT NOT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE UNIQUE INDEX a_registro_problemas_publicacion_id_IDX USING BTREE ON prisma.a_registro_problemas_publicacion (id,tipo_dato,tipo_dato_2,tipo_dato_3);



CREATE TABLE prisma.a_registro_cambios_fuente (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NULL,
	valor TEXT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE INDEX a_registro_cambios_fuente_id_IDX USING BTREE ON prisma.a_registro_cambios_fuente (id,tipo_dato,tipo_dato_2,tipo_dato_3);

CREATE TABLE prisma.a_registro_problemas_fuente (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NOT NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NOT NULL,
	valor TEXT NOT NULL,
  origen_antiguo varchar(100) NOT NULL,
	valor_antiguo TEXT NOT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE UNIQUE INDEX a_registro_problemas_fuente_id_IDX USING BTREE ON prisma.a_registro_problemas_fuente (id,tipo_dato,tipo_dato_2,tipo_dato_3);


CREATE TABLE prisma.a_registro_cambios_editor (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NULL,
	valor TEXT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE INDEX a_registro_cambios_editor_id_IDX USING BTREE ON prisma.a_registro_cambios_editor (id,tipo_dato,tipo_dato_2,tipo_dato_3);

CREATE TABLE prisma.a_registro_problemas_editor (
	id INT NOT NULL,
  id_carga varchar(40) NOT NULL,
	tipo_dato varchar(100) NOT NULL,
	tipo_dato_2 varchar(100) NULL,
	tipo_dato_3 varchar(100) NULL,
	origen varchar(100) NOT NULL,
	valor TEXT NOT NULL,
  origen_antiguo varchar(100) NOT NULL,
	valor_antiguo TEXT NOT NULL,
	fecha DATETIME NOT NULL,
  comentario TEXT NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_spanish_ci;
CREATE UNIQUE INDEX a_registro_problemas_editor_id_IDX USING BTREE ON prisma.a_registro_problemas_editor (id,tipo_dato,tipo_dato_2,tipo_dato_3);