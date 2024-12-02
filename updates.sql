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