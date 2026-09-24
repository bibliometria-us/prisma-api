/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-11.7.2-MariaDB, for Win64 (AMD64)
--
-- Host: localhost    Database: prisma
-- ------------------------------------------------------
-- Server version	11.8.5-MariaDB-ubu2404

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;

--
-- Table structure for table `TABLE 115`
--

DROP TABLE IF EXISTS `TABLE 115`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `TABLE 115` (
  `Editorial` varchar(21) DEFAULT NULL,
  `ISSN` varchar(11) DEFAULT NULL,
  `eISSN` varchar(10) DEFAULT NULL,
  `Título` varchar(122) DEFAULT NULL,
  `Tipo` varchar(22) DEFAULT NULL,
  `Descuento` varchar(94) DEFAULT NULL,
  `Promotor` varchar(13) DEFAULT NULL,
  `idFuente` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_configuracion`
--

DROP TABLE IF EXISTS `a_configuracion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_configuracion` (
  `variable` varchar(25) NOT NULL,
  `valor` varchar(150) NOT NULL,
  `editable` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`variable`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_controlcambios`
--

DROP TABLE IF EXISTS `a_controlcambios`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_controlcambios` (
  `idCambio` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `responsable` varchar(50) DEFAULT NULL,
  `accion` tinyint(3) unsigned DEFAULT NULL COMMENT '1 modificación de investigador, 2 modicifación publicación',
  `identificador` int(10) NOT NULL,
  `comentario` text NOT NULL,
  `fechaCambio` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idCambio`),
  KEY `responsable` (`responsable`,`accion`),
  KEY `fechaCambio` (`fechaCambio`),
  KEY `identificador` (`identificador`)
) ENGINE=InnoDB AUTO_INCREMENT=1986015 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_nota`
--

DROP TABLE IF EXISTS `a_nota`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_nota` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(20) NOT NULL,
  `elemento_id` bigint(20) unsigned NOT NULL,
  `valor` text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tipo` (`tipo`,`elemento_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_spanish2_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_permisos`
--

DROP TABLE IF EXISTS `a_permisos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_permisos` (
  `idPermisos` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `uid` varchar(25) NOT NULL,
  `nombre` varchar(60) NOT NULL,
  `rol` tinyint(2) unsigned DEFAULT 2 COMMENT '0: Administrador, 1:Editor de biblioteca, 2: Solo ver',
  `identificador` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Dependerá del valor de rol.',
  `idgp` int(11) DEFAULT NULL COMMENT 'Identificador de usuario en Gestión de Proyectos',
  PRIMARY KEY (`idPermisos`),
  UNIQUE KEY `uid` (`uid`),
  UNIQUE KEY `uid_2` (`uid`),
  UNIQUE KEY `uid_3` (`uid`)
) ENGINE=InnoDB AUTO_INCREMENT=370 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_permisos_multiple`
--

DROP TABLE IF EXISTS `a_permisos_multiple`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_permisos_multiple` (
  `mail` varchar(100) NOT NULL,
  `permiso` varchar(30) NOT NULL,
  PRIMARY KEY (`mail`,`permiso`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_problemas`
--

DROP TABLE IF EXISTS `a_problemas`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_problemas` (
  `idCarga` varchar(40) NOT NULL,
  `tipo_problema` varchar(100) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `id_dato` varchar(100) NOT NULL,
  `mensaje` text NOT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `antigua_fuente` varchar(100) DEFAULT NULL,
  `antiguo_valor` varchar(100) NOT NULL,
  `nueva_fuente` varchar(100) NOT NULL,
  `nuevo_valor` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_cambios_editor`
--

DROP TABLE IF EXISTS `a_registro_cambios_editor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_cambios_editor` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) DEFAULT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) DEFAULT NULL,
  `valor` text DEFAULT NULL,
  `valor_antiguo` varchar(100) DEFAULT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  `autor` varchar(100) DEFAULT NULL,
  KEY `a_registro_cambios_editor_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_cambios_fuente`
--

DROP TABLE IF EXISTS `a_registro_cambios_fuente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_cambios_fuente` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) DEFAULT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) DEFAULT NULL,
  `valor` text DEFAULT NULL,
  `valor_antiguo` varchar(100) DEFAULT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  `autor` varchar(100) DEFAULT NULL,
  KEY `a_registro_cambios_fuente_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_cambios_investigador`
--

DROP TABLE IF EXISTS `a_registro_cambios_investigador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_cambios_investigador` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) DEFAULT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) DEFAULT NULL,
  `valor` text DEFAULT NULL,
  `valor_antiguo` varchar(100) DEFAULT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  `autor` varchar(100) DEFAULT NULL,
  KEY `a_registro_cambios_publicacion_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_cambios_publicacion`
--

DROP TABLE IF EXISTS `a_registro_cambios_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_cambios_publicacion` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) DEFAULT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) DEFAULT NULL,
  `valor` text DEFAULT NULL,
  `valor_antiguo` varchar(100) DEFAULT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  `autor` varchar(100) DEFAULT NULL,
  KEY `a_registro_cambios_publicacion_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_problemas_editor`
--

DROP TABLE IF EXISTS `a_registro_problemas_editor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_problemas_editor` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) NOT NULL,
  `valor` text NOT NULL,
  `origen_antiguo` varchar(100) NOT NULL,
  `valor_antiguo` text NOT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  UNIQUE KEY `a_registro_problemas_editor_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_problemas_fuente`
--

DROP TABLE IF EXISTS `a_registro_problemas_fuente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_problemas_fuente` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) NOT NULL,
  `valor` text NOT NULL,
  `origen_antiguo` varchar(100) NOT NULL,
  `valor_antiguo` text NOT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  UNIQUE KEY `a_registro_problemas_fuente_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_problemas_investigador`
--

DROP TABLE IF EXISTS `a_registro_problemas_investigador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_problemas_investigador` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) NOT NULL,
  `valor` text NOT NULL,
  `origen_antiguo` varchar(100) NOT NULL,
  `valor_antiguo` text NOT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  UNIQUE KEY `a_registro_problemas_publicacion_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_registro_problemas_publicacion`
--

DROP TABLE IF EXISTS `a_registro_problemas_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_registro_problemas_publicacion` (
  `id` int(11) NOT NULL,
  `id_carga` varchar(40) NOT NULL,
  `tipo_dato` varchar(100) NOT NULL,
  `tipo_dato_2` varchar(100) DEFAULT NULL,
  `tipo_dato_3` varchar(100) DEFAULT NULL,
  `origen` varchar(100) NOT NULL,
  `valor` text NOT NULL,
  `origen_antiguo` varchar(100) NOT NULL,
  `valor_antiguo` text NOT NULL,
  `fecha` datetime NOT NULL,
  `comentario` text NOT NULL,
  UNIQUE KEY `a_registro_problemas_publicacion_id_IDX` (`id`,`tipo_dato`,`tipo_dato_2`,`tipo_dato_3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_responsable`
--

DROP TABLE IF EXISTS `a_responsable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_responsable` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `responsable_id` int(10) unsigned DEFAULT NULL,
  `centro_id` varchar(4) DEFAULT NULL,
  `usuario_gp_id` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idPermiso` (`responsable_id`),
  KEY `idCentro` (`centro_id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `a_tarea_pendiente`
--

DROP TABLE IF EXISTS `a_tarea_pendiente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `a_tarea_pendiente` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `responsable` varchar(20) NOT NULL,
  `tipo` varchar(30) NOT NULL,
  `valor` varchar(500) NOT NULL,
  `email` varchar(40) NOT NULL,
  `fechaEntrada` timestamp NOT NULL DEFAULT current_timestamp(),
  `respuesta` tinyint(4) DEFAULT NULL,
  `fechaResuelto` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `uid` (`responsable`,`tipo`),
  KEY `fechaResuelto` (`fechaResuelto`)
) ENGINE=InnoDB AUTO_INCREMENT=3095 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `aprobadas_2023`
--

DROP TABLE IF EXISTS `aprobadas_2023`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `aprobadas_2023` (
  `ID_Aprobada` int(3) DEFAULT NULL,
  `Fecha` varchar(10) DEFAULT NULL,
  `Mes` varchar(10) DEFAULT NULL,
  `Editorial` varchar(10) DEFAULT NULL,
  `Nombre` varchar(20) DEFAULT NULL,
  `Apellidos` varchar(27) DEFAULT NULL,
  `Correo` varchar(23) DEFAULT NULL,
  `Figura` varchar(45) DEFAULT NULL,
  `Figura (Agrupada)` varchar(43) DEFAULT NULL,
  `Grupo` varchar(9) DEFAULT NULL,
  `Género` varchar(9) DEFAULT NULL,
  `ID Prisma` varchar(4) DEFAULT NULL,
  `Otro autor` varchar(35) DEFAULT NULL,
  `Figura Otro autor` varchar(33) DEFAULT NULL,
  `ID Prisma Otro autor` varchar(4) DEFAULT NULL,
  `Departamento` varchar(65) DEFAULT NULL,
  `Área de conocimiento` varchar(52) DEFAULT NULL,
  `Biblioteca` varchar(22) DEFAULT NULL,
  `Centro` varchar(42) DEFAULT NULL,
  `Ramas de conocimiento` varchar(27) DEFAULT NULL,
  `Tipo de revista` varchar(11) DEFAULT NULL,
  `Tipo de artículo` varchar(29) DEFAULT NULL,
  `DOI` varchar(36) DEFAULT NULL,
  `CC` varchar(11) DEFAULT NULL,
  `ID_Revista` int(5) DEFAULT NULL,
  `Revista` varchar(90) DEFAULT NULL,
  `ISSN` varchar(9) DEFAULT NULL,
  `eISSN` varchar(9) DEFAULT NULL,
  `Título` varchar(224) DEFAULT NULL,
  `URL` varchar(55) DEFAULT NULL,
  `Precio` varchar(10) DEFAULT NULL,
  `Descuento` varchar(8) DEFAULT NULL,
  `APC` varchar(10) DEFAULT NULL,
  `JIF Mejor Cuartil` varchar(6) DEFAULT NULL,
  `CiteScore Mejor cuartil` varchar(12) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `cambios_editor`
--

DROP TABLE IF EXISTS `cambios_editor`;
/*!50001 DROP VIEW IF EXISTS `cambios_editor`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `cambios_editor` AS SELECT
 1 AS `responsable`,
  1 AS `identificador`,
  1 AS `comentario`,
  1 AS `fechaCambio` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cambios_fuente`
--

DROP TABLE IF EXISTS `cambios_fuente`;
/*!50001 DROP VIEW IF EXISTS `cambios_fuente`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `cambios_fuente` AS SELECT
 1 AS `responsable`,
  1 AS `identificador`,
  1 AS `comentario`,
  1 AS `fechaCambio` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cambios_publicacion`
--

DROP TABLE IF EXISTS `cambios_publicacion`;
/*!50001 DROP VIEW IF EXISTS `cambios_publicacion`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `cambios_publicacion` AS SELECT
 1 AS `responsable`,
  1 AS `identificador`,
  1 AS `comentario`,
  1 AS `fechaCambio` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `correo_lista_20251201`
--

DROP TABLE IF EXISTS `correo_lista_20251201`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `correo_lista_20251201` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `email` varchar(100) NOT NULL,
  `lista` varchar(10) NOT NULL,
  `añadido` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_lista` (`email`,`lista`)
) ENGINE=InnoDB AUTO_INCREMENT=9230 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `correo_lista_20260203`
--

DROP TABLE IF EXISTS `correo_lista_20260203`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `correo_lista_20260203` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `email` varchar(100) NOT NULL,
  `lista` varchar(100) NOT NULL,
  `añadido` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_lista` (`email`,`lista`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cvn_categoria_norm`
--

DROP TABLE IF EXISTS `cvn_categoria_norm`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `cvn_categoria_norm` (
  `id_categoria` varchar(6) NOT NULL,
  `nombre` varchar(150) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cvn_descarga`
--

DROP TABLE IF EXISTS `cvn_descarga`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `cvn_descarga` (
  `idDescarga` int(10) NOT NULL AUTO_INCREMENT,
  `responsable` varchar(50) NOT NULL,
  `tipo` int(1) NOT NULL COMMENT ' 1 descarga CVN, 2 descarga CVA ',
  `identificador` int(22) NOT NULL,
  `fechaDescarga` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idDescarga`),
  KEY `fechaDescarga` (`fechaDescarga`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=4170 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `eliminados_scholar`
--

DROP TABLE IF EXISTS `eliminados_scholar`;
/*!50001 DROP VIEW IF EXISTS `eliminados_scholar`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `eliminados_scholar` AS SELECT
 1 AS `biblioteca`,
  1 AS `fechaEliminacion`,
  1 AS `idInvestigador`,
  1 AS `nombre`,
  1 AS `apellidos`,
  1 AS `docuIden`,
  1 AS `email`,
  1 AS `idCategoria`,
  1 AS `idArea`,
  1 AS `fechaContratacion`,
  1 AS `idDepartamento`,
  1 AS `idCentro`,
  1 AS `nacionalidad`,
  1 AS `sexo`,
  1 AS `fechaNacimiento`,
  1 AS `fechaNombramiento`,
  1 AS `perfilPublico`,
  1 AS `fechaActualizacion` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `i_area`
--

DROP TABLE IF EXISTS `i_area`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_area` (
  `idArea` mediumint(3) unsigned zerofill NOT NULL,
  `nombre` varchar(150) NOT NULL,
  `idRama` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`idArea`),
  KEY `idxRama` (`idRama`),
  KEY `nombre_UNIQUE` (`nombre`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_biblioteca`
--

DROP TABLE IF EXISTS `i_biblioteca`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_biblioteca` (
  `idBiblioteca` tinyint(2) unsigned NOT NULL,
  `nombre` varchar(150) NOT NULL,
  PRIMARY KEY (`idBiblioteca`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_categoria`
--

DROP TABLE IF EXISTS `i_categoria`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_categoria` (
  `idCategoria` varchar(8) NOT NULL DEFAULT '',
  `nombre` varchar(50) DEFAULT NULL,
  `femenino` varchar(50) DEFAULT NULL,
  `tipo_pp` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL DEFAULT 'exc' COMMENT 'Tipo de usuario para los informes del plan propio. exc Excluido, cat Catedrático, mie Miembro, pre Predoctoral, pos Postdoctoral',
  PRIMARY KEY (`idCategoria`),
  KEY `idCategoria` (`idCategoria`),
  KEY `nombre` (`nombre`),
  KEY `femenino` (`femenino`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_centro`
--

DROP TABLE IF EXISTS `i_centro`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_centro` (
  `idCentro` varchar(5) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `idBiblioteca` tinyint(2) unsigned DEFAULT 0,
  `encargado` int(11) DEFAULT NULL,
  PRIMARY KEY (`idCentro`),
  KEY `biblioteca_idx` (`idBiblioteca`),
  KEY `nombre` (`nombre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_centro_mixto`
--

DROP TABLE IF EXISTS `i_centro_mixto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_centro_mixto` (
  `idCentroMixto` bigint(20) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(500) NOT NULL,
  `acronimo` varchar(20) DEFAULT NULL,
  `ambito` varchar(100) DEFAULT NULL,
  `resumen` text NOT NULL DEFAULT '',
  `url` text DEFAULT NULL,
  `fecha_creacion` date DEFAULT NULL,
  PRIMARY KEY (`idCentroMixto`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_centro_mixto_linea_investigacion`
--

DROP TABLE IF EXISTS `i_centro_mixto_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_centro_mixto_linea_investigacion` (
  `idCentroMixto` bigint(20) NOT NULL,
  `idLineaInvestigacion` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idCentroMixto`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_centro_mixto_palabra_clave`
--

DROP TABLE IF EXISTS `i_centro_mixto_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_centro_mixto_palabra_clave` (
  `idCentroMixto` bigint(20) NOT NULL,
  `idPalabraClave` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  `forzar_visible` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idCentroMixto`,`idPalabraClave`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_conjunto`
--

DROP TABLE IF EXISTS `i_conjunto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_conjunto` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tipo` varchar(50) NOT NULL COMMENT 'Instituto, Unidad de Excelencia, etc.',
  `nombre` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `acronimo` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci DEFAULT NULL COMMENT 'Nombre acortado',
  `responsable_id` int(11) DEFAULT NULL COMMENT 'Identificador del investigador responsable',
  PRIMARY KEY (`id`),
  KEY `tipo` (`tipo`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_departamento`
--

DROP TABLE IF EXISTS `i_departamento`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_departamento` (
  `idDepartamento` varchar(4) NOT NULL,
  `nombre` varchar(150) NOT NULL,
  PRIMARY KEY (`idDepartamento`),
  KEY `nombre` (`nombre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_doctorado`
--

DROP TABLE IF EXISTS `i_doctorado`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_doctorado` (
  `idDoctorado` smallint(4) NOT NULL DEFAULT 0,
  `nombre` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci DEFAULT NULL,
  `url` text NOT NULL,
  PRIMARY KEY (`idDoctorado`),
  KEY `nombre` (`nombre`(191))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_doctorado_linea_investigacion`
--

DROP TABLE IF EXISTS `i_doctorado_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_doctorado_linea_investigacion` (
  `idDoctorado` int(4) NOT NULL,
  `idLineaInvestigacion` int(8) NOT NULL,
  PRIMARY KEY (`idDoctorado`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_fecha_cese`
--

DROP TABLE IF EXISTS `i_fecha_cese`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_fecha_cese` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idMotivo` varchar(8) NOT NULL,
  `fechaCese` date NOT NULL,
  PRIMARY KEY (`idInvestigador`,`idMotivo`),
  UNIQUE KEY `idInves_UNIQUE` (`idInvestigador`),
  KEY `fk_motivocese_has_investigador_investigador1_idx` (`idInvestigador`),
  KEY `fk_motivocese_has_investigador_motivocese1_idx` (`idMotivo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_grupo`
--

DROP TABLE IF EXISTS `i_grupo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_grupo` (
  `idGrupo` varchar(10) NOT NULL,
  `nombre` varchar(200) NOT NULL,
  `acronimo` varchar(75) DEFAULT NULL COMMENT 'Acrónimo del grupo (viene de SISIUS)',
  `rama` varchar(4) DEFAULT NULL COMMENT 'Rama cientifíca en la que se engloba el grupo de investigación (viene de SISISUS)',
  `codigo` smallint(4) DEFAULT NULL,
  `institucion` varchar(200) DEFAULT NULL,
  `ambito` varchar(100) NOT NULL DEFAULT 'Andalucía',
  `resumen` text NOT NULL DEFAULT '',
  `fecha_creacion` date DEFAULT NULL,
  `estado` varchar(100) DEFAULT NULL,
  `situacion` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`idGrupo`),
  KEY `nombre` (`nombre`(191)),
  KEY `acronimo` (`acronimo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_grupo_investigador`
--

DROP TABLE IF EXISTS `i_grupo_investigador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_grupo_investigador` (
  `idInvestigador` int(10) NOT NULL,
  `idGrupo` varchar(10) NOT NULL,
  `rol` varchar(50) DEFAULT 'Miembro',
  `actualizado` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`idInvestigador`,`idGrupo`),
  UNIQUE KEY `idInvestigador` (`idInvestigador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_grupo_linea_investigacion`
--

DROP TABLE IF EXISTS `i_grupo_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_grupo_linea_investigacion` (
  `idGrupo` varchar(10) NOT NULL,
  `idLineaInvestigacion` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idGrupo`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_grupo_palabra_clave`
--

DROP TABLE IF EXISTS `i_grupo_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_grupo_palabra_clave` (
  `idGrupo` varchar(10) NOT NULL,
  `idPalabraClave` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  `forzar_visible` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idGrupo`,`idPalabraClave`),
  KEY `idGrupo` (`idGrupo`),
  KEY `idPalabraClave` (`idPalabraClave`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_identificador_investigador`
--

DROP TABLE IF EXISTS `i_identificador_investigador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_identificador_investigador` (
  `idIdentificador` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idInvestigador` int(10) unsigned NOT NULL,
  `tipo` varchar(45) NOT NULL COMMENT 'orcid, wos, scopus, dialnet, idus, scholar\n',
  `valor` varchar(100) NOT NULL,
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`idIdentificador`),
  UNIQUE KEY `tipo_2` (`tipo`,`valor`),
  KEY `investigador_idx` (`idInvestigador`),
  KEY `tipo` (`tipo`),
  KEY `valor` (`valor`),
  KEY `i_identificador_investigador_idInvestigador_IDX` (`idInvestigador`,`tipo`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=60119 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_identificador_wos`
--

DROP TABLE IF EXISTS `i_identificador_wos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_identificador_wos` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `investigador_id` int(10) unsigned NOT NULL,
  `valor` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tipo_2` (`valor`),
  KEY `investigador_idx` (`investigador_id`),
  KEY `valor` (`valor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_institucion_colectivo`
--

DROP TABLE IF EXISTS `i_institucion_colectivo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_institucion_colectivo` (
  `idInstitucion` bigint(20) NOT NULL,
  `idColectivo` bigint(20) NOT NULL,
  `tipo` varchar(50) NOT NULL,
  PRIMARY KEY (`idInstitucion`,`idColectivo`,`tipo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_instituto`
--

DROP TABLE IF EXISTS `i_instituto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_instituto` (
  `idInstituto` tinyint(2) unsigned NOT NULL AUTO_INCREMENT,
  `nombre` varchar(200) NOT NULL,
  `acronimo` varchar(20) NOT NULL,
  `ambito` varchar(100) DEFAULT NULL,
  `resumen` text NOT NULL DEFAULT '',
  `url` text DEFAULT NULL,
  `fecha_creacion` date DEFAULT NULL,
  PRIMARY KEY (`idInstituto`),
  KEY `nombre` (`nombre`(191)),
  KEY `acronimo` (`acronimo`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_instituto_linea_investigacion`
--

DROP TABLE IF EXISTS `i_instituto_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_instituto_linea_investigacion` (
  `idInstituto` bigint(20) NOT NULL,
  `idLineaInvestigacion` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idInstituto`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_instituto_palabra_clave`
--

DROP TABLE IF EXISTS `i_instituto_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_instituto_palabra_clave` (
  `idInstituto` bigint(20) NOT NULL,
  `idPalabraClave` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  `forzar_visible` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idInstituto`,`idPalabraClave`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_investigador`
--

DROP TABLE IF EXISTS `i_investigador`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_investigador` (
  `idInvestigador` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'Identificador del investigador dentro de la base de datos de bibliometría',
  `nombre` varchar(75) NOT NULL COMMENT 'Nombre propio del investigador',
  `apellidos` varchar(150) NOT NULL COMMENT 'Apellido o apellidos del investigador',
  `docuIden` varchar(45) NOT NULL COMMENT 'Documento de identidad (dni, pasaporte, etc.)',
  `email` varchar(75) DEFAULT NULL COMMENT 'Correo electrónico del investigador. Debería ser del dominio us.es',
  `idCategoria` varchar(8) NOT NULL,
  `idArea` mediumint(3) unsigned zerofill NOT NULL,
  `fechaContratacion` date DEFAULT NULL COMMENT 'Primera fecha de contratación',
  `idDepartamento` varchar(4) NOT NULL,
  `idCentro` varchar(5) NOT NULL,
  `idCentroCenso` varchar(5) DEFAULT NULL,
  `nacionalidad` varchar(30) DEFAULT NULL,
  `sexo` tinyint(4) DEFAULT NULL,
  `resumen` text NOT NULL DEFAULT '',
  `fechaNacimiento` date DEFAULT NULL,
  `fechaNombramiento` date DEFAULT NULL,
  `perfilPublico` tinyint(1) NOT NULL DEFAULT 1,
  `fechaActualizacion` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT 'Fecha de la última actualización del registro',
  PRIMARY KEY (`idInvestigador`),
  UNIQUE KEY `docuIden_UNIQUE` (`docuIden`),
  UNIQUE KEY `docuIden` (`docuIden`),
  UNIQUE KEY `email_UNIQUE` (`email`),
  KEY `idxCategoria` (`idCategoria`),
  KEY `idxDepartamento` (`idDepartamento`),
  KEY `idxCentro` (`idCentro`),
  KEY `idxArea` (`idArea`),
  KEY `nombre` (`nombre`),
  KEY `apellidos` (`apellidos`),
  KEY `i_investigador_idCentroCenso_IDX` (`idCentroCenso`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=10824 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `i_investigador_activo`
--

DROP TABLE IF EXISTS `i_investigador_activo`;
/*!50001 DROP VIEW IF EXISTS `i_investigador_activo`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `i_investigador_activo` AS SELECT
 1 AS `idInvestigador`,
  1 AS `nombre`,
  1 AS `apellidos`,
  1 AS `docuIden`,
  1 AS `email`,
  1 AS `idCategoria`,
  1 AS `fechaNombramiento`,
  1 AS `idArea`,
  1 AS `fechaContratacion`,
  1 AS `idDepartamento`,
  1 AS `idCentro`,
  1 AS `idCentroCenso`,
  1 AS `sexo`,
  1 AS `resumen`,
  1 AS `nacionalidad`,
  1 AS `fechaNacimiento`,
  1 AS `perfilPublico`,
  1 AS `fechaActualizacion` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `i_investigador_excluido`
--

DROP TABLE IF EXISTS `i_investigador_excluido`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_investigador_excluido` (
  `idInvestigador` int(10) NOT NULL,
  `excluido` tinyint(1) DEFAULT NULL COMMENT '0: Siempre admitido. 1: Siempre excluido',
  PRIMARY KEY (`idInvestigador`),
  KEY `excluido` (`excluido`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_investigador_linea_investigacion`
--

DROP TABLE IF EXISTS `i_investigador_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_investigador_linea_investigacion` (
  `idInvestigador` int(10) NOT NULL,
  `idLineaInvestigacion` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idInvestigador`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_investigador_palabra_clave`
--

DROP TABLE IF EXISTS `i_investigador_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_investigador_palabra_clave` (
  `idInvestigador` int(10) NOT NULL,
  `idPalabraClave` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idInvestigador`,`idPalabraClave`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_linea_investigacion`
--

DROP TABLE IF EXISTS `i_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_linea_investigacion` (
  `idLineaInvestigacion` bigint(20) NOT NULL AUTO_INCREMENT,
  `nombre` text NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idLineaInvestigacion`)
) ENGINE=InnoDB AUTO_INCREMENT=5013 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_linea_investigacion_doctorado`
--

DROP TABLE IF EXISTS `i_linea_investigacion_doctorado`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_linea_investigacion_doctorado` (
  `idLineaInvestigacion` int(8) NOT NULL,
  `nombre` varchar(500) NOT NULL,
  PRIMARY KEY (`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_miembro_centro_mixto`
--

DROP TABLE IF EXISTS `i_miembro_centro_mixto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_miembro_centro_mixto` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idCentroMixto` tinyint(2) unsigned NOT NULL,
  `rol` varchar(100) NOT NULL DEFAULT 'Miembro ordinario',
  `actualizado` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`idInvestigador`),
  KEY `idxInstituto` (`idInvestigador`),
  KEY `idxInvestigador` (`idCentroMixto`),
  KEY `rol` (`rol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_miembro_conjunto`
--

DROP TABLE IF EXISTS `i_miembro_conjunto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_miembro_conjunto` (
  `investigador_id` int(11) NOT NULL,
  `conjunto_id` bigint(20) NOT NULL,
  PRIMARY KEY (`investigador_id`,`conjunto_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_miembro_instituto`
--

DROP TABLE IF EXISTS `i_miembro_instituto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_miembro_instituto` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idInstituto` tinyint(2) unsigned NOT NULL,
  `rol` varchar(100) NOT NULL DEFAULT 'Miembro ordinario',
  `actualizado` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`idInvestigador`),
  KEY `idxInstituto` (`idInvestigador`),
  KEY `idxInvestigador` (`idInstituto`),
  KEY `rol` (`rol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_miembro_instituto_conjunto`
--

DROP TABLE IF EXISTS `i_miembro_instituto_conjunto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_miembro_instituto_conjunto` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idInstituto` tinyint(2) unsigned NOT NULL,
  `rol` varchar(100) NOT NULL DEFAULT 'Miembro ordinario',
  `actualizado` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`idInvestigador`),
  KEY `idxInstituto` (`idInvestigador`),
  KEY `idxInvestigador` (`idInstituto`),
  KEY `rol` (`rol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_miembro_unidad_excelencia`
--

DROP TABLE IF EXISTS `i_miembro_unidad_excelencia`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_miembro_unidad_excelencia` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idUdExcelencia` tinyint(2) unsigned NOT NULL,
  `rol` varchar(100) NOT NULL DEFAULT 'Miembro ordinario',
  `actualizado` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`idInvestigador`),
  KEY `idxInstituto` (`idInvestigador`),
  KEY `idxInvestigador` (`idUdExcelencia`),
  KEY `rol` (`rol`),
  KEY `idInvestigador` (`idInvestigador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_motivo_cese`
--

DROP TABLE IF EXISTS `i_motivo_cese`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_motivo_cese` (
  `idMotivo` varchar(8) NOT NULL,
  `nombre` varchar(250) NOT NULL,
  PRIMARY KEY (`idMotivo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_palabra_clave`
--

DROP TABLE IF EXISTS `i_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_palabra_clave` (
  `idPalabraClave` bigint(20) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(100) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idPalabraClave`)
) ENGINE=InnoDB AUTO_INCREMENT=6362 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_profesor_doctorado`
--

DROP TABLE IF EXISTS `i_profesor_doctorado`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_profesor_doctorado` (
  `idInvestigador` int(10) NOT NULL DEFAULT 0,
  `idDoctorado` smallint(4) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idInvestigador`,`idDoctorado`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_profesor_doctorado_20231201`
--

DROP TABLE IF EXISTS `i_profesor_doctorado_20231201`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_profesor_doctorado_20231201` (
  `idInvestigador` int(10) NOT NULL DEFAULT 0,
  `idDoctorado` smallint(4) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idInvestigador`,`idDoctorado`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_profesor_doctorado_202312011120`
--

DROP TABLE IF EXISTS `i_profesor_doctorado_202312011120`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_profesor_doctorado_202312011120` (
  `idInvestigador` int(10) NOT NULL DEFAULT 0,
  `idDoctorado` smallint(4) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_profesor_doctorado_202312011121`
--

DROP TABLE IF EXISTS `i_profesor_doctorado_202312011121`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_profesor_doctorado_202312011121` (
  `idInvestigador` int(10) NOT NULL DEFAULT 0,
  `idDoctorado` smallint(4) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_profesor_doctorado_linea_inv`
--

DROP TABLE IF EXISTS `i_profesor_doctorado_linea_inv`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_profesor_doctorado_linea_inv` (
  `idInvestigador` int(10) NOT NULL DEFAULT 0,
  `idLineaInvestigacion` int(8) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`idInvestigador`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_rama`
--

DROP TABLE IF EXISTS `i_rama`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_rama` (
  `idRama` tinyint(2) unsigned NOT NULL AUTO_INCREMENT,
  `nombre` varchar(150) NOT NULL,
  `padre` tinyint(2) unsigned DEFAULT 0 COMMENT '0 cuando es una rama fundamental',
  PRIMARY KEY (`idRama`),
  UNIQUE KEY `nombre_UNIQUE` (`nombre`),
  KEY `fk_i_rama_i_rama1_idx` (`padre`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_rama_us`
--

DROP TABLE IF EXISTS `i_rama_us`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_rama_us` (
  `idDepartamento` varchar(4) NOT NULL,
  `idArea` smallint(3) unsigned zerofill NOT NULL,
  `idRama` tinyint(2) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`idDepartamento`,`idArea`,`idRama`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_rama_us_20230601`
--

DROP TABLE IF EXISTS `i_rama_us_20230601`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_rama_us_20230601` (
  `idDepartamento` varchar(4) NOT NULL,
  `idArea` smallint(3) unsigned zerofill NOT NULL,
  `idRama` tinyint(3) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`idDepartamento`,`idArea`,`idRama`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_rama_us_nueva`
--

DROP TABLE IF EXISTS `i_rama_us_nueva`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_rama_us_nueva` (
  `idDepartamento` varchar(4) NOT NULL,
  `idArea` smallint(3) unsigned zerofill NOT NULL,
  `idRama` tinyint(2) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`idDepartamento`,`idArea`,`idRama`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_rama_us_nueva_20230601`
--

DROP TABLE IF EXISTS `i_rama_us_nueva_20230601`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_rama_us_nueva_20230601` (
  `Departamento` varchar(99) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `idDepartamento` varchar(4) NOT NULL,
  `Área` varchar(58) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `idArea` smallint(3) unsigned zerofill NOT NULL,
  `Rama` varchar(29) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `idRama` tinyint(2) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`idDepartamento`,`idArea`,`idRama`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio`
--

DROP TABLE IF EXISTS `i_sexenio`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `investigador_id` bigint(20) NOT NULL,
  `inicio` varchar(4) NOT NULL,
  `fin` varchar(4) NOT NULL,
  `transferencia` tinyint(1) NOT NULL,
  `entrada_vigor` varchar(4) NOT NULL,
  `id_categoria` varchar(8) NOT NULL,
  `nomina` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `investigador_id_2` (`investigador_id`,`inicio`,`fin`,`transferencia`),
  KEY `investigador_id` (`investigador_id`),
  KEY `inicio` (`inicio`,`fin`),
  KEY `transferencia` (`transferencia`)
) ENGINE=InnoDB AUTO_INCREMENT=95583 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio_20230602`
--

DROP TABLE IF EXISTS `i_sexenio_20230602`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio_20230602` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `investigador_id` bigint(20) NOT NULL,
  `inicio` varchar(4) NOT NULL,
  `fin` varchar(4) NOT NULL,
  `transferencia` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `investigador_id` (`investigador_id`),
  KEY `inicio` (`inicio`,`fin`),
  KEY `transferencia` (`transferencia`)
) ENGINE=InnoDB AUTO_INCREMENT=6378 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio_20230921`
--

DROP TABLE IF EXISTS `i_sexenio_20230921`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio_20230921` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `investigador_id` bigint(20) NOT NULL,
  `inicio` varchar(4) NOT NULL,
  `fin` varchar(4) NOT NULL,
  `transferencia` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `investigador_id` (`investigador_id`),
  KEY `inicio` (`inicio`,`fin`),
  KEY `transferencia` (`transferencia`)
) ENGINE=InnoDB AUTO_INCREMENT=34109 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio_20251125`
--

DROP TABLE IF EXISTS `i_sexenio_20251125`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio_20251125` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `investigador_id` bigint(20) NOT NULL,
  `inicio` varchar(4) NOT NULL,
  `fin` varchar(4) NOT NULL,
  `transferencia` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `investigador_id_2` (`investigador_id`,`inicio`,`fin`,`transferencia`),
  KEY `inicio` (`inicio`,`fin`) USING BTREE,
  KEY `investigador_id` (`investigador_id`) USING BTREE,
  KEY `transferencia` (`transferencia`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=58342 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio_20260714`
--

DROP TABLE IF EXISTS `i_sexenio_20260714`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio_20260714` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `investigador_id` bigint(20) NOT NULL,
  `inicio` varchar(4) NOT NULL,
  `fin` varchar(4) NOT NULL,
  `transferencia` tinyint(1) NOT NULL,
  `entrada_vigor` varchar(4) NOT NULL,
  `id_categoria` varchar(8) NOT NULL,
  `nomina` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `investigador_id_2` (`investigador_id`,`inicio`,`fin`,`transferencia`),
  KEY `inicio` (`inicio`,`fin`) USING BTREE,
  KEY `investigador_id` (`investigador_id`) USING BTREE,
  KEY `transferencia` (`transferencia`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=87369 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_sexenio_informes`
--

DROP TABLE IF EXISTS `i_sexenio_informes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_sexenio_informes` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `idPublicacion` bigint(20) DEFAULT NULL,
  `perfilPrisma` varchar(250) DEFAULT NULL,
  `email` varchar(250) DEFAULT NULL,
  `aportacion` int(11) DEFAULT NULL,
  `urlPrismaAport` varchar(500) DEFAULT NULL,
  `titulo` varchar(500) DEFAULT NULL,
  `autoria` varchar(500) DEFAULT NULL,
  `anio` int(11) DEFAULT NULL,
  `doi` varchar(100) DEFAULT NULL,
  `repositorio` varchar(200) DEFAULT NULL,
  `preferente` tinyint(1) DEFAULT NULL,
  `contribucionNotas` text DEFAULT NULL,
  `resumen` text DEFAULT NULL,
  `impactoCientifico` text DEFAULT NULL,
  `impactoSocial` text DEFAULT NULL,
  `contribucion` text DEFAULT NULL,
  `impactoFinal` text DEFAULT NULL,
  `timestamp` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `usuario_generador` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2077 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_uca1400_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER inserts_before
BEFORE INSERT
ON i_sexenio_informes FOR EACH ROW
BEGIN
    SET NEW.timestamp = NOW();
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `i_unidad_excelencia`
--

DROP TABLE IF EXISTS `i_unidad_excelencia`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_unidad_excelencia` (
  `idUdExcelencia` bigint(20) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `acronimo` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci DEFAULT NULL COMMENT 'Nombre acortado',
  `ambito` varchar(100) DEFAULT NULL,
  `resumen` text NOT NULL,
  `url` text DEFAULT NULL,
  `fecha_creacion` date NOT NULL,
  PRIMARY KEY (`idUdExcelencia`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_unidad_excelencia_linea_investigacion`
--

DROP TABLE IF EXISTS `i_unidad_excelencia_linea_investigacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_unidad_excelencia_linea_investigacion` (
  `idUdExcelencia` bigint(20) NOT NULL,
  `idLineaInvestigacion` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  PRIMARY KEY (`idUdExcelencia`,`idLineaInvestigacion`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `i_unidad_excelencia_palabra_clave`
--

DROP TABLE IF EXISTS `i_unidad_excelencia_palabra_clave`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `i_unidad_excelencia_palabra_clave` (
  `idUdExcelencia` bigint(20) NOT NULL,
  `idPalabraClave` bigint(20) NOT NULL,
  `fecha` date DEFAULT NULL,
  `forzar_visible` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idUdExcelencia`,`idPalabraClave`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `institucion`
--

DROP TABLE IF EXISTS `institucion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `institucion` (
  `idInstitucion` bigint(20) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(200) NOT NULL,
  `id_ror` varchar(9) DEFAULT NULL,
  `tipo_organizacion` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `enlace` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `acronimos` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `coordenadas.lat` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `coordenadas.lng` varchar(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `ciudad` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `pais` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `fundref_preferido` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`idInstitucion`),
  UNIQUE KEY `id_ror` (`id_ror`)
) ENGINE=InnoDB AUTO_INCREMENT=18034 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `investigador_biblioteca`
--

DROP TABLE IF EXISTS `investigador_biblioteca`;
/*!50001 DROP VIEW IF EXISTS `investigador_biblioteca`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `investigador_biblioteca` AS SELECT
 1 AS `idInvestigador`,
  1 AS `biblioteca` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `investigador_edad`
--

DROP TABLE IF EXISTS `investigador_edad`;
/*!50001 DROP VIEW IF EXISTS `investigador_edad`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `investigador_edad` AS SELECT
 1 AS `id`,
  1 AS `apellidos`,
  1 AS `nombre`,
  1 AS `edad`,
  1 AS `fechaNacimiento`,
  1 AS `email` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `m_at`
--

DROP TABLE IF EXISTS `m_at`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_at` (
  `idFuente` int(9) NOT NULL,
  `titulo` varchar(500) DEFAULT NULL,
  `editorial` varchar(100) NOT NULL,
  `tipo` varchar(100) NOT NULL,
  `descuento` int(3) DEFAULT NULL,
  `licencias_limitadas` tinyint(1) DEFAULT 0,
  `promotor` varchar(100) DEFAULT NULL,
  `agno` int(11) DEFAULT NULL,
  UNIQUE KEY `m_at_idFuente_IDX` (`idFuente`,`agno`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_cea_apq`
--

DROP TABLE IF EXISTS `m_cea_apq`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_cea_apq` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `idFuente` int(10) NOT NULL,
  `coleccion` varchar(200) DEFAULT NULL,
  `universidad` varchar(160) DEFAULT NULL,
  `convocatoria` varchar(22) DEFAULT NULL,
  `agno` varchar(4) DEFAULT NULL,
  `internacionalidad` tinyint(1) DEFAULT NULL,
  `fecha_expiracion` date DEFAULT NULL,
  `url` varchar(133) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `monografia` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `monografia` (`monografia`)
) ENGINE=InnoDB AUTO_INCREMENT=307 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_citescore`
--

DROP TABLE IF EXISTS `m_citescore`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_citescore` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `revista` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `categoria` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `citeScore` decimal(6,3) NOT NULL DEFAULT 0.000,
  `posicion` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `decil` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `tercil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `issn_2` (`issn`,`agno`,`categoria`),
  KEY `issn` (`issn`),
  KEY `agno` (`agno`),
  KEY `revista` (`revista`(255)),
  KEY `revista_2` (`revista`(255),`agno`),
  KEY `idFuente` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=1409697 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_citescore_insert` BEFORE INSERT ON `m_citescore`
FOR EACH ROW BEGIN
    DECLARE found_idFuente INT;

    
    SET found_idFuente = (
        SELECT f.idFuente
        FROM p_fuente f
        JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
        ON idf.idFuente = f.idFuente
        WHERE NEW.revista = titulo
        LIMIT 1
    );

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn = idf.valor
            LIMIT 1
        );
    END IF;

    
    SET NEW.idFuente = found_idFuente;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_citescore_20250708`
--

DROP TABLE IF EXISTS `m_citescore_20250708`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_citescore_20250708` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `revista` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `categoria` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `citeScore` decimal(6,3) NOT NULL DEFAULT 0.000,
  `posicion` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `decil` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `tercil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `issn_2` (`issn`,`agno`,`categoria`),
  KEY `issn` (`issn`),
  KEY `agno` (`agno`),
  KEY `revista` (`revista`(255)),
  KEY `revista_2` (`revista`(255),`agno`),
  KEY `idFuente` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=1155521 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_citescore_20250708_insert` BEFORE INSERT ON `m_citescore_20250708`
FOR EACH ROW BEGIN
    DECLARE found_idFuente INT;

    
    SET found_idFuente = (
        SELECT f.idFuente
        FROM p_fuente f
        JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
        ON idf.idFuente = f.idFuente
        WHERE NEW.revista = titulo
        LIMIT 1
    );

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn = idf.valor
            LIMIT 1
        );
    END IF;

    
    SET NEW.idFuente = found_idFuente;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_csic`
--

DROP TABLE IF EXISTS `m_csic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_csic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `editorial` varchar(179) DEFAULT NULL,
  `puntuacion` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `editorial` (`editorial`)
) ENGINE=InnoDB AUTO_INCREMENT=5817 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_fecyt`
--

DROP TABLE IF EXISTS `m_fecyt`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_fecyt` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `titulo` varchar(160) DEFAULT NULL,
  `issn` varchar(11) DEFAULT NULL,
  `eissn` varchar(9) DEFAULT NULL,
  `url` varchar(133) DEFAULT NULL,
  `convocatoria` varchar(22) DEFAULT NULL,
  `igualdad` tinyint(1) DEFAULT NULL,
  `agno` varchar(4) DEFAULT NULL,
  `categoria` varchar(52) DEFAULT NULL,
  `puntuacion` decimal(4,2) DEFAULT NULL,
  `posicion` varchar(5) DEFAULT NULL,
  `cuartil` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `titulo` (`titulo`,`convocatoria`,`agno`,`categoria`) USING BTREE,
  KEY `issn` (`issn`),
  KEY `eissn` (`eissn`),
  KEY `issn_2` (`issn`,`eissn`),
  KEY `agno` (`agno`)
) ENGINE=InnoDB AUTO_INCREMENT=5408 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_fecyt_nueva`
--

DROP TABLE IF EXISTS `m_fecyt_nueva`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_fecyt_nueva` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `titulo` varchar(160) DEFAULT NULL,
  `issn` varchar(11) DEFAULT NULL,
  `eissn` varchar(9) DEFAULT NULL,
  `url` varchar(133) DEFAULT NULL,
  `convocatoria` varchar(22) DEFAULT NULL,
  `agno` varchar(4) DEFAULT NULL,
  `categoria` varchar(52) DEFAULT NULL,
  `puntuacion` varchar(5) DEFAULT NULL,
  `posicion` varchar(5) DEFAULT NULL,
  `cuartil` varchar(2) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `issn` (`issn`),
  KEY `eissn` (`eissn`),
  KEY `issn_2` (`issn`,`eissn`)
) ENGINE=InnoDB AUTO_INCREMENT=847 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_fecyt_old`
--

DROP TABLE IF EXISTS `m_fecyt_old`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_fecyt_old` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `titulo` varchar(500) NOT NULL,
  `issn` varchar(15) DEFAULT NULL,
  `eissn` varchar(15) DEFAULT NULL,
  `puntuacion` decimal(4,2) NOT NULL,
  `cuartil` varchar(2) NOT NULL,
  `agno` varchar(4) NOT NULL,
  `categoria` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=394 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_idr`
--

DROP TABLE IF EXISTS `m_idr`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_idr` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `titulo` varchar(250) DEFAULT NULL,
  `dialnet_id` int(11) DEFAULT NULL,
  `prisma_id` bigint(20) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `anualidad` int(11) DEFAULT NULL,
  `categoria` varchar(50) DEFAULT NULL,
  `factorImpacto` decimal(4,3) DEFAULT NULL,
  `cuartil` int(11) DEFAULT NULL,
  `percentil` int(11) DEFAULT NULL,
  `posicion` int(11) DEFAULT NULL,
  `totalRevista` int(11) NOT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `anualidad_categoria_idFuente` (`anualidad`,`categoria`,`idFuente`) USING BTREE,
  KEY `anualidad` (`anualidad`),
  KEY `idFuente` (`idFuente`),
  KEY `categoria` (`categoria`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=59928 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_idr_20230613`
--

DROP TABLE IF EXISTS `m_idr_20230613`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_idr_20230613` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `titulo` varchar(250) DEFAULT NULL,
  `dialnet_id` int(11) DEFAULT NULL,
  `prisma_id` bigint(20) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `anualidad` int(11) DEFAULT NULL,
  `categoria` varchar(50) DEFAULT NULL,
  `factorImpacto` decimal(4,3) DEFAULT NULL,
  `cuartil` int(11) DEFAULT NULL,
  `percentil` int(11) DEFAULT NULL,
  `posicion` int(11) DEFAULT NULL,
  `totalRevista` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `anualidad` (`anualidad`)
) ENGINE=InnoDB AUTO_INCREMENT=23426 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_idr_20231114`
--

DROP TABLE IF EXISTS `m_idr_20231114`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_idr_20231114` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `titulo` varchar(250) DEFAULT NULL,
  `dialnet_id` int(11) DEFAULT NULL,
  `prisma_id` bigint(20) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `anualidad` int(11) DEFAULT NULL,
  `categoria` varchar(50) DEFAULT NULL,
  `factorImpacto` decimal(4,3) DEFAULT NULL,
  `cuartil` int(11) DEFAULT NULL,
  `percentil` int(11) DEFAULT NULL,
  `posicion` int(11) DEFAULT NULL,
  `totalRevista` int(11) NOT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `anualidad` (`anualidad`),
  KEY `idFuente` (`idFuente`),
  KEY `idFuente_2` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=49339 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_idr_20241030`
--

DROP TABLE IF EXISTS `m_idr_20241030`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_idr_20241030` (
  `id` bigint(20) NOT NULL DEFAULT 0,
  `titulo` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `dialnet_id` int(11) DEFAULT NULL,
  `prisma_id` bigint(20) NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `anualidad` int(11) DEFAULT NULL,
  `categoria` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `factorImpacto` decimal(4,3) DEFAULT NULL,
  `cuartil` int(11) DEFAULT NULL,
  `percentil` int(11) DEFAULT NULL,
  `posicion` int(11) DEFAULT NULL,
  `totalRevista` int(11) NOT NULL,
  `idFuente` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_idr_20251210`
--

DROP TABLE IF EXISTS `m_idr_20251210`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_idr_20251210` (
  `id` bigint(20) NOT NULL DEFAULT 0,
  `titulo` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `dialnet_id` int(11) DEFAULT NULL,
  `prisma_id` bigint(20) NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `anualidad` int(11) DEFAULT NULL,
  `categoria` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `factorImpacto` decimal(4,3) DEFAULT NULL,
  `cuartil` int(11) DEFAULT NULL,
  `percentil` int(11) DEFAULT NULL,
  `posicion` int(11) DEFAULT NULL,
  `totalRevista` int(11) NOT NULL,
  `idFuente` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_informes`
--

DROP TABLE IF EXISTS `m_informes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_informes` (
  `idMetrica` int(11) NOT NULL AUTO_INCREMENT,
  `ambito` varchar(15) NOT NULL,
  `identificador` varchar(10) NOT NULL,
  `basedatos` varchar(10) NOT NULL,
  `tipo` varchar(15) NOT NULL,
  `valor` varchar(20) NOT NULL,
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `identificadorInt` int(11) DEFAULT NULL,
  PRIMARY KEY (`idMetrica`),
  KEY `ambito` (`ambito`),
  KEY `tipo` (`tipo`),
  KEY `identificador` (`identificador`),
  KEY `basedatos` (`basedatos`,`tipo`) USING BTREE,
  KEY `ambito_2` (`ambito`,`tipo`),
  KEY `m_informes_identificadorInt_IDX` (`identificadorInt`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=141040 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_informes_update_identificadorInt` BEFORE INSERT ON `m_informes` FOR EACH ROW BEGIN
    SET NEW.identificadorInt = NULL;
    IF NEW.identificador REGEXP '^[0-9]+$' THEN
        SET NEW.identificadorInt = CAST(NEW.identificador AS INT);
    END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_informes_insert_identificadorInt` BEFORE UPDATE ON `m_informes` FOR EACH ROW BEGIN
    SET NEW.identificadorInt = NULL;
    IF NEW.identificador REGEXP '^[0-9]+$' THEN
        SET NEW.identificadorInt = CAST(NEW.identificador AS INT);
    END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_jci`
--

DROP TABLE IF EXISTS `m_jci`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_jci` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `revista` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `categoria` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `jci` decimal(6,3) NOT NULL DEFAULT 0.000,
  `posicion` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `decil` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `tercil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `percentil` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `agno_2` (`agno`,`categoria`,`idFuente`),
  KEY `issn` (`issn`),
  KEY `agno` (`agno`),
  KEY `idFuente` (`idFuente`),
  KEY `idx_jci_issn` (`issn`,`issn_2`)
) ENGINE=InnoDB AUTO_INCREMENT=541908 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_jcr`
--

DROP TABLE IF EXISTS `m_jcr`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_jcr` (
  `id_jcr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `edition` varchar(100) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(8) DEFAULT NULL,
  `tercil` varchar(8) DEFAULT NULL,
  `percentile` varchar(10) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_jcr`),
  UNIQUE KEY `year_2` (`year`,`edition`,`category`,`idFuente`) USING BTREE,
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `impact_factor` (`impact_factor`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `issn_2` (`issn`,`year`,`edition`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`edition`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `journal_2` (`journal`(191),`year`),
  KEY `journal_3` (`journal`(191),`issn`,`issn_2`),
  KEY `idFuente` (`idFuente`),
  KEY `idx_jcr_issn` (`issn`,`issn_2`)
) ENGINE=InnoDB AUTO_INCREMENT=1395522 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_jcr_20240117`
--

DROP TABLE IF EXISTS `m_jcr_20240117`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_jcr_20240117` (
  `id_jcr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `edition` varchar(100) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(8) DEFAULT NULL,
  `tercil` varchar(8) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_jcr`),
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `impact_factor` (`impact_factor`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `issn_2` (`issn`,`year`,`edition`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`edition`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `journal_2` (`journal`(191),`year`),
  KEY `journal_3` (`journal`(191),`issn`,`issn_2`),
  KEY `idFuente` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=478470 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_jcr_20240205`
--

DROP TABLE IF EXISTS `m_jcr_20240205`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_jcr_20240205` (
  `id_jcr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `edition` varchar(100) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(8) DEFAULT NULL,
  `tercil` varchar(8) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_jcr`),
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `impact_factor` (`impact_factor`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `issn_2` (`issn`,`year`,`edition`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`edition`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `journal_2` (`journal`(191),`year`),
  KEY `journal_3` (`journal`(191),`issn`,`issn_2`),
  KEY `idFuente` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=498615 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_jcr_insert` BEFORE INSERT ON `m_jcr_20240205` FOR EACH ROW BEGIN
    DECLARE found_idFuente INT;

    
    SET found_idFuente = (
        SELECT f.idFuente
        FROM p_fuente f
        JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
        ON idf.idFuente = f.idFuente
        WHERE NEW.journal = titulo
        LIMIT 1
    );

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn = idf.valor
            LIMIT 1
        );
    END IF;

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn_2 = idf.valor
            LIMIT 1
        );
    END IF;

    
    SET NEW.idFuente = found_idFuente;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_jcr_20250709`
--

DROP TABLE IF EXISTS `m_jcr_20250709`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_jcr_20250709` (
  `id_jcr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) NOT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `edition` varchar(100) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(8) DEFAULT NULL,
  `tercil` varchar(8) DEFAULT NULL,
  `percentile` varchar(10) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_jcr`),
  UNIQUE KEY `year_2` (`year`,`edition`,`category`,`idFuente`) USING BTREE,
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `impact_factor` (`impact_factor`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `issn_2` (`issn`,`year`,`edition`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`edition`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `journal_2` (`journal`(191),`year`),
  KEY `journal_3` (`journal`(191),`issn`,`issn_2`),
  KEY `idFuente` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=1313061 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_publicaciones`
--

DROP TABLE IF EXISTS `m_publicaciones`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_publicaciones` (
  `idMetrica` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idPublicacion` int(10) unsigned NOT NULL,
  `metrica` varchar(25) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `basedatos` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `valor` varchar(25) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`idMetrica`),
  KEY `idxPublicacion` (`idPublicacion`)
) ENGINE=InnoDB AUTO_INCREMENT=435804 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Métricas de las publicaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scholar`
--

DROP TABLE IF EXISTS `m_scholar`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scholar` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScholar` varchar(15) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `pag_us` tinyint(1) NOT NULL DEFAULT 0,
  `citasTotal` int(10) NOT NULL,
  `citasHace5` int(10) NOT NULL,
  `indiceHTotal` int(10) NOT NULL,
  `indiceHHace5` int(10) NOT NULL,
  `indiceI10Total` int(10) NOT NULL,
  `indiceI10Hace5` int(10) NOT NULL,
  `nDocs` int(10) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scholar_20230515`
--

DROP TABLE IF EXISTS `m_scholar_20230515`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scholar_20230515` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScholar` varchar(15) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `pag_us` tinyint(1) NOT NULL DEFAULT 0,
  `citasTotal` int(10) NOT NULL,
  `citasHace5` int(10) NOT NULL,
  `indiceHTotal` int(10) NOT NULL,
  `indiceHHace5` int(10) NOT NULL,
  `indiceI10Total` int(10) NOT NULL,
  `indiceI10Hace5` int(10) NOT NULL,
  `nDocs` int(10) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT current_timestamp(),
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idScholar`,`fecha`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scholar_20230613`
--

DROP TABLE IF EXISTS `m_scholar_20230613`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scholar_20230613` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScholar` varchar(15) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `pag_us` tinyint(1) NOT NULL DEFAULT 0,
  `citasTotal` int(10) NOT NULL,
  `citasHace5` int(10) NOT NULL,
  `indiceHTotal` int(10) NOT NULL,
  `indiceHHace5` int(10) NOT NULL,
  `indiceI10Total` int(10) NOT NULL,
  `indiceI10Hace5` int(10) NOT NULL,
  `nDocs` int(10) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scholar_20231113`
--

DROP TABLE IF EXISTS `m_scholar_20231113`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scholar_20231113` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScholar` varchar(15) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `pag_us` tinyint(1) NOT NULL DEFAULT 0,
  `citasTotal` int(10) NOT NULL,
  `citasHace5` int(10) NOT NULL,
  `indiceHTotal` int(10) NOT NULL,
  `indiceHHace5` int(10) NOT NULL,
  `indiceI10Total` int(10) NOT NULL,
  `indiceI10Hace5` int(10) NOT NULL,
  `nDocs` int(10) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scholar_20231218`
--

DROP TABLE IF EXISTS `m_scholar_20231218`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scholar_20231218` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScholar` varchar(15) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `pag_us` tinyint(1) NOT NULL DEFAULT 0,
  `citasTotal` int(10) NOT NULL,
  `citasHace5` int(10) NOT NULL,
  `indiceHTotal` int(10) NOT NULL,
  `indiceHHace5` int(10) NOT NULL,
  `indiceI10Total` int(10) NOT NULL,
  `indiceI10Hace5` int(10) NOT NULL,
  `nDocs` int(10) NOT NULL,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus`
--

DROP TABLE IF EXISTS `m_scopus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus_20230918`
--

DROP TABLE IF EXISTS `m_scopus_20230918`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus_20230918` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus_20231016`
--

DROP TABLE IF EXISTS `m_scopus_20231016`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus_20231016` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus_20240214`
--

DROP TABLE IF EXISTS `m_scopus_20240214`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus_20240214` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus_20240318`
--

DROP TABLE IF EXISTS `m_scopus_20240318`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus_20240318` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_scopus_AAAAMMDD`
--

DROP TABLE IF EXISTS `m_scopus_AAAAMMDD`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_scopus_AAAAMMDD` (
  `idInvestigador` int(10) unsigned NOT NULL,
  `idScopus` varchar(45) NOT NULL,
  `citation_count` int(10) NOT NULL DEFAULT 0,
  `cited_by_count` int(10) NOT NULL DEFAULT 0,
  `coauthor_count` int(10) NOT NULL DEFAULT 0,
  `document_count` int(10) NOT NULL DEFAULT 0,
  `h_index` int(10) NOT NULL DEFAULT 0,
  `fecha` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `comentario` varchar(200) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_sjr`
--

DROP TABLE IF EXISTS `m_sjr`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_sjr` (
  `id_sjr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) DEFAULT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(5) DEFAULT NULL,
  `tercil` varchar(5) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_sjr`),
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `impact_factor` (`impact_factor`),
  KEY `issn_2` (`issn`,`year`,`category`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`category`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `idFuente` (`idFuente`),
  KEY `year_2` (`year`,`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=2854100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_sjr_insert` BEFORE INSERT ON `m_sjr`
FOR EACH ROW BEGIN
    DECLARE found_idFuente INT;

    
    SET found_idFuente = (
        SELECT f.idFuente
        FROM p_fuente f
        JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
        ON idf.idFuente = f.idFuente
        WHERE NEW.journal = titulo
        LIMIT 1
    );

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn = idf.valor
            LIMIT 1
        );
    END IF;

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn_2 = idf.valor
            LIMIT 1
        );
    END IF;

    
    SET NEW.idFuente = found_idFuente;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_sjr_20230510`
--

DROP TABLE IF EXISTS `m_sjr_20230510`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_sjr_20230510` (
  `id_sjr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) DEFAULT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(5) DEFAULT NULL,
  `tercil` varchar(5) DEFAULT NULL,
  PRIMARY KEY (`id_sjr`),
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `impact_factor` (`impact_factor`),
  KEY `issn_2` (`issn`,`year`,`category`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`category`),
  KEY `year` (`year`)
) ENGINE=InnoDB AUTO_INCREMENT=2290996 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_sjr_20250708`
--

DROP TABLE IF EXISTS `m_sjr_20250708`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_sjr_20250708` (
  `id_sjr` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `journal` varchar(500) DEFAULT NULL,
  `issn` varchar(9) DEFAULT NULL,
  `issn_2` varchar(9) DEFAULT NULL,
  `year` varchar(4) NOT NULL,
  `category` varchar(75) NOT NULL,
  `impact_factor` decimal(6,3) DEFAULT 0.000,
  `rank` varchar(10) DEFAULT NULL,
  `quartile` varchar(10) DEFAULT NULL,
  `decil` varchar(5) DEFAULT NULL,
  `tercil` varchar(5) DEFAULT NULL,
  `idFuente` int(11) DEFAULT NULL,
  PRIMARY KEY (`id_sjr`),
  KEY `issn` (`issn`,`year`),
  KEY `category` (`category`),
  KEY `quartile` (`quartile`),
  KEY `decil` (`decil`),
  KEY `tercil` (`tercil`),
  KEY `impact_factor` (`impact_factor`),
  KEY `issn_2` (`issn`,`year`,`category`),
  KEY `issn_2_2` (`issn_2`,`year`),
  KEY `issn_2_3` (`issn_2`,`year`,`category`),
  KEY `year` (`year`),
  KEY `journal` (`journal`(191)),
  KEY `idFuente` (`idFuente`),
  KEY `year_2` (`year`,`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=2446884 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER `tr_m_sjr_20250708_insert` BEFORE INSERT ON `m_sjr_20250708`
FOR EACH ROW BEGIN
    DECLARE found_idFuente INT;

    
    SET found_idFuente = (
        SELECT f.idFuente
        FROM p_fuente f
        JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
        ON idf.idFuente = f.idFuente
        WHERE NEW.journal = titulo
        LIMIT 1
    );

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn = idf.valor
            LIMIT 1
        );
    END IF;

    
    IF found_idFuente IS NULL THEN
        SET found_idFuente = (
            SELECT f.idFuente
            FROM p_fuente f
            JOIN (SELECT * FROM p_identificador_fuente WHERE tipo IN ("issn", "eissn")) idf
            ON idf.idFuente = f.idFuente
            WHERE NEW.issn_2 = idf.valor
            LIMIT 1
        );
    END IF;

    
    SET NEW.idFuente = found_idFuente;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `m_spi`
--

DROP TABLE IF EXISTS `m_spi`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_spi` (
  `idMetrica` int(11) NOT NULL AUTO_INCREMENT,
  `editorial` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `editorial_original` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '2018',
  `categoria` varchar(35) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `puntuacion` float NOT NULL,
  `ambito` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `posicion` smallint(6) NOT NULL,
  `total_ed` smallint(6) NOT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`idMetrica`),
  KEY `editorial` (`editorial`),
  KEY `agno` (`agno`)
) ENGINE=InnoDB AUTO_INCREMENT=10963 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_spi_20230522`
--

DROP TABLE IF EXISTS `m_spi_20230522`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_spi_20230522` (
  `idMetrica` int(11) NOT NULL AUTO_INCREMENT,
  `editorial` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `editorial_original` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '2018',
  `categoria` varchar(35) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `puntuacion` float NOT NULL,
  `ambito` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `posicion` smallint(6) NOT NULL,
  `total_ed` smallint(6) NOT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`idMetrica`),
  KEY `editorial` (`editorial`),
  KEY `agno` (`agno`)
) ENGINE=InnoDB AUTO_INCREMENT=10827 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `m_spi_20230920`
--

DROP TABLE IF EXISTS `m_spi_20230920`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `m_spi_20230920` (
  `idMetrica` int(11) NOT NULL AUTO_INCREMENT,
  `editorial` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `editorial_original` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '2018',
  `categoria` varchar(35) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `puntuacion` float NOT NULL,
  `ambito` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `posicion` smallint(6) NOT NULL,
  `total_ed` smallint(6) NOT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`idMetrica`),
  KEY `editorial` (`editorial`),
  KEY `agno` (`agno`)
) ENGINE=InnoDB AUTO_INCREMENT=10827 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_acceso_abierto`
--

DROP TABLE IF EXISTS `p_acceso_abierto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_acceso_abierto` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `publicacion_id` bigint(20) unsigned NOT NULL,
  `valor` varchar(50) NOT NULL,
  `origen` varchar(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `pub_valor` (`valor`,`publicacion_id`),
  KEY `publicacion_id` (`publicacion_id`)
) ENGINE=InnoDB AUTO_INCREMENT=163465 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_afiliacion`
--

DROP TABLE IF EXISTS `p_afiliacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_afiliacion` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `afiliacion` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `pais` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `scopus_id` int(11) DEFAULT NULL COMMENT 'Identificador de la afiliación en Scopus',
  `vease` bigint(20) DEFAULT NULL COMMENT 'Identificador de la afiliación normalizada',
  `nombre_ror` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `id_ror` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `afiliacion` (`afiliacion`(191)),
  KEY `pais` (`pais`),
  KEY `scopus` (`scopus_id`),
  KEY `afiliacion_2` (`afiliacion`(191),`pais`)
) ENGINE=InnoDB AUTO_INCREMENT=50757 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci COMMENT='Tabla con las afiliaciones de los autores';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_autor`
--

DROP TABLE IF EXISTS `p_autor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_autor` (
  `idAutor` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `orden` smallint(3) NOT NULL COMMENT 'Orden de la firma en la publicación',
  `firma` varchar(250) NOT NULL COMMENT 'Firma del autor en la publicación',
  `rol` varchar(20) NOT NULL COMMENT 'Rol en la publicación: autor, editor, etc.',
  `contacto` varchar(1) NOT NULL DEFAULT 'N',
  `idPublicacion` int(10) unsigned NOT NULL,
  `idInvestigador` int(10) unsigned DEFAULT 0 COMMENT 'Identificador en la tabla ''investigador''. 0 si no es un autor US',
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `eliminado` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`idAutor`),
  KEY `idInvestigador` (`idInvestigador`),
  KEY `idPublicacion` (`idPublicacion`),
  KEY `idx_autor_rol_orden` (`rol`,`orden`)
) ENGINE=InnoDB AUTO_INCREMENT=1811391 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Autores de las publicaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_autor_afiliacion`
--

DROP TABLE IF EXISTS `p_autor_afiliacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_autor_afiliacion` (
  `autor_id` bigint(20) NOT NULL COMMENT 'Identificador de la tabla p_autor',
  `afiliacion_id` bigint(20) NOT NULL COMMENT 'Identificador de la tabla p_afiliacion',
  PRIMARY KEY (`autor_id`,`afiliacion_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci COMMENT='Guarda la relación entre un autor y sus afiliaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_autor_bak_limpieza`
--

DROP TABLE IF EXISTS `p_autor_bak_limpieza`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_autor_bak_limpieza` (
  `idAutor` int(10) unsigned NOT NULL DEFAULT 0,
  `orden` smallint(3) NOT NULL COMMENT 'Orden de la firma en la publicación',
  `firma` varchar(250) NOT NULL COMMENT 'Firma del autor en la publicación',
  `rol` varchar(20) NOT NULL COMMENT 'Rol en la publicación: autor, editor, etc.',
  `contacto` varchar(1) NOT NULL DEFAULT 'N',
  `idPublicacion` int(10) unsigned NOT NULL,
  `idInvestigador` int(10) unsigned DEFAULT 0 COMMENT 'Identificador en la tabla ''investigador''. 0 si no es un autor US',
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `eliminado` tinyint(1) DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_dato_fuente`
--

DROP TABLE IF EXISTS `p_dato_fuente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_dato_fuente` (
  `idIdentificador` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `idFuente` bigint(10) unsigned NOT NULL,
  `tipo` varchar(10) NOT NULL,
  `valor` varchar(150) NOT NULL,
  `comentario` varchar(500) DEFAULT NULL,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`idIdentificador`),
  KEY `valor` (`valor`),
  KEY `tipo` (`tipo`),
  KEY `fuente1_idx` (`idFuente`)
) ENGINE=InnoDB AUTO_INCREMENT=56851 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Datos de las fuentes de las publicaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_dato_publicacion`
--

DROP TABLE IF EXISTS `p_dato_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_dato_publicacion` (
  `idDato` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(100) NOT NULL,
  `valor` varchar(300) NOT NULL,
  `idPublicacion` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idDato`),
  KEY `fk_p_dato_publicacion_p_publicacion1_idx` (`idPublicacion`),
  KEY `tipo` (`tipo`),
  KEY `valor` (`valor`(191))
) ENGINE=InnoDB AUTO_INCREMENT=847590 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_dato_publicacion_bak_limpieza`
--

DROP TABLE IF EXISTS `p_dato_publicacion_bak_limpieza`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_dato_publicacion_bak_limpieza` (
  `idDato` int(10) unsigned NOT NULL DEFAULT 0,
  `tipo` varchar(100) NOT NULL,
  `valor` varchar(300) NOT NULL,
  `idPublicacion` int(10) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_editor`
--

DROP TABLE IF EXISTS `p_editor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_editor` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `nombre` varchar(200) NOT NULL,
  `tipo` varchar(50) NOT NULL DEFAULT 'Otros',
  `vease` bigint(20) DEFAULT NULL COMMENT 'Identificador de la editorial/editor normalizada',
  `pais` varchar(50) NOT NULL DEFAULT 'Desconocido',
  `url` varchar(260) DEFAULT NULL COMMENT 'URL de la editorial',
  `visible` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `nombre` (`nombre`(191)),
  KEY `tipo` (`tipo`),
  KEY `pais` (`pais`)
) ENGINE=InnoDB AUTO_INCREMENT=14720 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Editoriales de las fuentes';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_editor_mal`
--

DROP TABLE IF EXISTS `p_editor_mal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_editor_mal` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `nombre` varchar(200) NOT NULL,
  `tipo` varchar(50) NOT NULL DEFAULT 'Otros',
  `vease` bigint(20) DEFAULT NULL COMMENT 'Identificador de la editorial/editor normalizada',
  `pais` varchar(50) NOT NULL DEFAULT 'Desconocido',
  `url` varchar(260) DEFAULT NULL COMMENT 'URL de la editorial',
  `visible` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `nombre` (`nombre`(191)),
  KEY `tipo` (`tipo`),
  KEY `pais` (`pais`)
) ENGINE=InnoDB AUTO_INCREMENT=10659 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Editoriales de las fuentes';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_fecha_publicacion`
--

DROP TABLE IF EXISTS `p_fecha_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_fecha_publicacion` (
  `idPublicacion` int(11) NOT NULL,
  `tipo` varchar(100) NOT NULL,
  `mes` int(11) DEFAULT NULL,
  `agno` int(11) NOT NULL,
  `dia` int(11) DEFAULT NULL,
  KEY `p_fecha_publicacion_idPublicacion_IDX` (`idPublicacion`,`tipo`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_financiacion`
--

DROP TABLE IF EXISTS `p_financiacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_financiacion` (
  `idFinanciacion` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `codigo` varchar(50) DEFAULT NULL,
  `agencia` varchar(300) DEFAULT NULL,
  `publicacion_id` int(10) unsigned NOT NULL,
  `idProyecto` int(15) DEFAULT NULL,
  `eliminado` tinyint(1) DEFAULT 0,
  PRIMARY KEY (`idFinanciacion`),
  KEY `codigo` (`codigo`)
) ENGINE=InnoDB AUTO_INCREMENT=196860 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_financiacion_20260526`
--

DROP TABLE IF EXISTS `p_financiacion_20260526`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_financiacion_20260526` (
  `idFinanciacion` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `codigo` varchar(50) DEFAULT NULL,
  `agencia` varchar(300) DEFAULT NULL,
  `publicacion_id` int(10) unsigned NOT NULL,
  `idProyecto` int(15) DEFAULT NULL,
  PRIMARY KEY (`idFinanciacion`),
  KEY `codigo` (`codigo`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=190060 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_fuente`
--

DROP TABLE IF EXISTS `p_fuente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_fuente` (
  `idFuente` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(25) NOT NULL COMMENT 'revista, libro,...',
  `titulo` varchar(800) NOT NULL,
  `editorial` varchar(200) DEFAULT NULL,
  `origen` varchar(50) NOT NULL,
  `validado` tinyint(1) DEFAULT 1,
  `fechaActualizacion` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `eliminado` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idFuente`),
  KEY `titulo` (`titulo`(191)),
  KEY `tipo` (`tipo`)
) ENGINE=InnoDB AUTO_INCREMENT=76697 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Fuente de la publicación';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_identificador_fuente`
--

DROP TABLE IF EXISTS `p_identificador_fuente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_identificador_fuente` (
  `idIdentificador` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `idFuente` bigint(10) unsigned NOT NULL,
  `tipo` varchar(10) NOT NULL COMMENT 'eissn, eisbn, isbn, issn, doi, wos, etc',
  `valor` varchar(50) NOT NULL,
  `origen` varchar(20) DEFAULT NULL,
  `comentario` varchar(500) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`idIdentificador`),
  KEY `valor` (`valor`),
  KEY `tipo` (`tipo`),
  KEY `fuente1_idx` (`idFuente`),
  KEY `idx_pif_valor_tipo` (`valor`,`tipo`)
) ENGINE=InnoDB AUTO_INCREMENT=107200 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Identificadores de las fuentes de las publicaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_identificador_publicacion`
--

DROP TABLE IF EXISTS `p_identificador_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_identificador_publicacion` (
  `idIdentificador` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idPublicacion` int(10) unsigned DEFAULT NULL,
  `tipo` varchar(10) NOT NULL COMMENT 'doi,scopus, wos, idus',
  `valor` varchar(100) NOT NULL,
  `origen` varchar(20) DEFAULT NULL,
  `comentario` varchar(500) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idIdentificador`),
  UNIQUE KEY `tipo_2` (`tipo`,`valor`),
  KEY `valor` (`valor`),
  KEY `tipo` (`tipo`),
  KEY `fk_p_identificador_publicacion_p_publicacion1_idx` (`idPublicacion`)
) ENGINE=InnoDB AUTO_INCREMENT=641038 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci COMMENT='Identificadores de las publicaciones';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_identificador_publicacion_bak_limpieza`
--

DROP TABLE IF EXISTS `p_identificador_publicacion_bak_limpieza`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_identificador_publicacion_bak_limpieza` (
  `idIdentificador` int(10) unsigned NOT NULL DEFAULT 0,
  `idPublicacion` int(10) unsigned DEFAULT NULL,
  `tipo` varchar(10) NOT NULL COMMENT 'doi,scopus, wos, idus',
  `valor` varchar(100) NOT NULL,
  `origen` varchar(20) DEFAULT NULL,
  `comentario` varchar(500) DEFAULT NULL,
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `p_publicacion`
--

DROP TABLE IF EXISTS `p_publicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_publicacion` (
  `idPublicacion` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(50) NOT NULL COMMENT 'Tipo de publicación: artículo, nota, revisión, ponencia',
  `titulo` varchar(1000) NOT NULL,
  `agno` varchar(4) NOT NULL,
  `idFuente` int(10) unsigned NOT NULL DEFAULT 0,
  `origen` varchar(50) NOT NULL,
  `validado` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'Indica si la publicación ha sido validada',
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `eliminado` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`idPublicacion`),
  KEY `titulo` (`titulo`(191)),
  KEY `fuente1_idx` (`idFuente`),
  KEY `tipo` (`tipo`),
  KEY `agno` (`agno`),
  KEY `idPublicacion` (`idPublicacion`,`agno`,`validado`,`eliminado`)
) ENGINE=InnoDB AUTO_INCREMENT=289692 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_uca1400_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER prisma.trg_p_publicacion_tipo_bi
BEFORE INSERT ON prisma.p_publicacion
FOR EACH ROW
BEGIN
    DECLARE v_activo     TINYINT DEFAULT NULL;
    DECLARE v_tipo_nuevo VARCHAR(100) DEFAULT NULL;

    -- Check if current type is active
    SELECT 1 INTO v_activo
    FROM config.tipos_publicacion
    WHERE nombre = NEW.tipo AND activo = 1
    LIMIT 1;

    -- If not active, attempt mapping
    IF v_activo IS NULL THEN
        SELECT tipo_nuevo INTO v_tipo_nuevo
        FROM config.map_tipos_publicacion_antiguos
        WHERE tipo_antiguo = NEW.tipo
        LIMIT 1;

        IF v_tipo_nuevo IS NOT NULL THEN
            IF v_tipo_nuevo = 'Eliminar' THEN
                -- Prevent insertion by raising a custom error
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'Cannot insert: publication type is marked as "Eliminar".';
            ELSE
                -- Mutate the field directly before saving to disk
                SET NEW.tipo = v_tipo_nuevo;
            END IF;
        END IF;
        -- If no mapping exists (v_tipo_nuevo IS NULL), keep original NEW.tipo
    END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_uca1400_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER prisma.trg_p_publicacion_tipo_au
AFTER UPDATE ON prisma.p_publicacion
FOR EACH ROW
BEGIN
    DECLARE v_activo     TINYINT DEFAULT NULL;
    DECLARE v_tipo_nuevo VARCHAR(100) DEFAULT NULL;

    IF NOT (NEW.tipo <=> OLD.tipo) THEN

        SELECT 1 INTO v_activo
        FROM config.tipos_publicacion
        WHERE nombre = NEW.tipo AND activo = 1
        LIMIT 1;

        IF v_activo IS NULL THEN
            SELECT tipo_nuevo INTO v_tipo_nuevo
            FROM config.map_tipos_publicacion_antiguos
            WHERE tipo_antiguo = NEW.tipo
            LIMIT 1;

            IF v_tipo_nuevo IS NOT NULL THEN
                IF v_tipo_nuevo = 'Eliminar' THEN
                    DELETE FROM prisma.p_identificador_publicacion
                    WHERE idPublicacion = NEW.idPublicacion;

                    DELETE FROM prisma.p_dato_publicacion
                    WHERE idPublicacion = NEW.idPublicacion;

                    DELETE FROM prisma.p_autor
                    WHERE idPublicacion = NEW.idPublicacion;

                    DELETE FROM prisma.p_publicacion
                    WHERE idPublicacion = NEW.idPublicacion;
                ELSE
                    UPDATE prisma.p_publicacion
                    SET tipo = v_tipo_nuevo
                    WHERE idPublicacion = NEW.idPublicacion;
                END IF;
            END IF;
        END IF;

    END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_unicode_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER delete_p_identificador_publicacion
AFTER DELETE ON p_publicacion FOR EACH ROW
BEGIN
    DELETE FROM p_identificador_publicacion
    WHERE idPublicacion = OLD.idPublicacion;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `p_publicacion_bak_limpieza`
--

DROP TABLE IF EXISTS `p_publicacion_bak_limpieza`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `p_publicacion_bak_limpieza` (
  `idPublicacion` int(10) unsigned NOT NULL DEFAULT 0,
  `tipo` varchar(50) NOT NULL COMMENT 'Tipo de publicación: artículo, nota, revisión, ponencia',
  `titulo` varchar(1000) NOT NULL,
  `agno` varchar(4) NOT NULL,
  `idFuente` int(10) unsigned NOT NULL DEFAULT 0,
  `origen` varchar(50) NOT NULL,
  `validado` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'Indica si la publicación ha sido validada',
  `fechaActualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `eliminado` tinyint(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `publicacionesXcentro`
--

DROP TABLE IF EXISTS `publicacionesXcentro`;
/*!50001 DROP VIEW IF EXISTS `publicacionesXcentro`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `publicacionesXcentro` AS SELECT
 1 AS `idPublicacion`,
  1 AS `tipo`,
  1 AS `titulo`,
  1 AS `agno`,
  1 AS `idFuente`,
  1 AS `origen`,
  1 AS `validado`,
  1 AS `fechaActualizacion`,
  1 AS `eliminado`,
  1 AS `idCentro` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `r_pbi`
--

DROP TABLE IF EXISTS `r_pbi`;
/*!50001 DROP VIEW IF EXISTS `r_pbi`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `r_pbi` AS SELECT
 1 AS `NOMBRE`,
  1 AS `APELLIDOS`,
  1 AS `ID_DEPARTAMENTO`,
  1 AS `DEPARTAMENTO`,
  1 AS `ID_GRUPO`,
  1 AS `GRUPO`,
  1 AS `ID_CENTRO`,
  1 AS `CENTRO`,
  1 AS `ID_INSTITUTO`,
  1 AS `INSTITUTO`,
  1 AS `Id_Prisma`,
  1 AS `Id_Scopus`,
  1 AS `Id_Wos`,
  1 AS `Id_Openalex`,
  1 AS `N_Pubs_Total_Scopus`,
  1 AS `N_Total_Citas_Total_Scopus`,
  1 AS `Indice_H_Total_Scopus`,
  1 AS `N_Pubs_10_Scopus`,
  1 AS `N_Total_Citas_10_Scopus`,
  1 AS `Indice_H_10_Scopus`,
  1 AS `N_Pubs_5_Scopus`,
  1 AS `N_Total_Citas_5_Scopus`,
  1 AS `Indice_H_5_Scopus`,
  1 AS `N_Pubs_3_Scopus`,
  1 AS `N_Total_Citas_3_Scopus`,
  1 AS `Indice_H_3_Scopus`,
  1 AS `N_Pubs_Total_Wos`,
  1 AS `N_Total_Citas_Total_Wos`,
  1 AS `Indice_H_Total_Wos`,
  1 AS `N_Pubs_10_Wos`,
  1 AS `N_Total_Citas_10_Wos`,
  1 AS `Indice_H_10_Wos`,
  1 AS `N_Pubs_5_Wos`,
  1 AS `N_Total_Citas_5_Wos`,
  1 AS `Indice_H_5_Wos`,
  1 AS `N_Pubs_3_Wos`,
  1 AS `N_Total_Citas_3_Wos`,
  1 AS `Indice_H_3_Wos`,
  1 AS `Rama` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `ramas_prueba`
--

DROP TABLE IF EXISTS `ramas_prueba`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ramas_prueba` (
  `Id` int(4) NOT NULL DEFAULT 0,
  `Departamento` varchar(99) DEFAULT NULL,
  `Área` varchar(58) DEFAULT NULL,
  `Rama` varchar(29) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  KEY `Departamento` (`Departamento`),
  KEY `Área` (`Área`),
  KEY `Rama` (`Rama`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `ranking_scholar`
--

DROP TABLE IF EXISTS `ranking_scholar`;
/*!50001 DROP VIEW IF EXISTS `ranking_scholar`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `ranking_scholar` AS SELECT
 1 AS `Id`,
  1 AS `idScholar`,
  1 AS `nombre`,
  1 AS `idDepartamento`,
  1 AS `idArea`,
  1 AS `idRamaAneca`,
  1 AS `idRama`,
  1 AS `nDocs`,
  1 AS `citasTotal`,
  1 AS `indiceHTotal`,
  1 AS `indiceI10Total`,
  1 AS `citasHace5`,
  1 AS `indiceHHace5`,
  1 AS `indiceI10Hace5` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ranking_scholar_mejorado`
--

DROP TABLE IF EXISTS `ranking_scholar_mejorado`;
/*!50001 DROP VIEW IF EXISTS `ranking_scholar_mejorado`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `ranking_scholar_mejorado` AS SELECT
 1 AS `Id`,
  1 AS `idScholar`,
  1 AS `nombre`,
  1 AS `idDepartamento`,
  1 AS `idArea`,
  1 AS `idRamaAneca`,
  1 AS `idRama`,
  1 AS `nDocs`,
  1 AS `citasTotal`,
  1 AS `indiceHTotal`,
  1 AS `indiceI10Total`,
  1 AS `citasHace5`,
  1 AS `indiceHHace5`,
  1 AS `indiceI10Hace5` */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ranking_scopus`
--

DROP TABLE IF EXISTS `ranking_scopus`;
/*!50001 DROP VIEW IF EXISTS `ranking_scopus`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `ranking_scopus` AS SELECT
 1 AS `idInves`,
  1 AS `idScopus`,
  1 AS `nombre`,
  1 AS `idDepartamento`,
  1 AS `idArea`,
  1 AS `idRama`,
  1 AS `idRamaAneca`,
  1 AS `citation_count`,
  1 AS `cited_by_count`,
  1 AS `coauthor_count`,
  1 AS `document_count`,
  1 AS `h_index` */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `re`
--

DROP TABLE IF EXISTS `re`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `re` (
  `id` bigint(20) unsigned NOT NULL DEFAULT 0,
  `revista` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `issn` varchar(9) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `agno` varchar(4) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `categoria` varchar(75) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `citeScore` decimal(6,3) NOT NULL DEFAULT 0.000,
  `posicion` varchar(15) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `cuartil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `decil` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `tercil` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary table structure for view `tesis_fecha_lectura_mal`
--

DROP TABLE IF EXISTS `tesis_fecha_lectura_mal`;
/*!50001 DROP VIEW IF EXISTS `tesis_fecha_lectura_mal`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `tesis_fecha_lectura_mal` AS SELECT
 1 AS `URL`,
  1 AS `fecha_fectura` */;
SET character_set_client = @saved_cs_client;

--
-- Dumping routines for database 'prisma'
--
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sp_eliminar_publicacion` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_uca1400_ai_ci */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `sp_eliminar_publicacion`(
    IN  p_idPublicacion INT UNSIGNED,
    OUT p_resultado     VARCHAR(255)
)
BEGIN
    DECLARE v_existe INT DEFAULT 0;
    DECLARE v_msg_error TEXT DEFAULT '';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_msg_error = MESSAGE_TEXT;
        ROLLBACK;
        SET p_resultado = CONCAT('ERROR: ', v_msg_error);
    END;

    SELECT COUNT(*) INTO v_existe
    FROM prisma.p_publicacion
    WHERE idPublicacion = p_idPublicacion;

    IF v_existe = 0 THEN
        SET p_resultado = CONCAT('ERROR: la publicacion ', p_idPublicacion, ' no existe');
    ELSE
        START TRANSACTION;

        DELETE FROM prisma.p_identificador_publicacion
        WHERE idPublicacion = p_idPublicacion;

        DELETE FROM prisma.p_dato_publicacion
        WHERE idPublicacion = p_idPublicacion;

        DELETE FROM prisma.p_autor
        WHERE idPublicacion = p_idPublicacion;

        DELETE FROM prisma.p_publicacion
        WHERE idPublicacion = p_idPublicacion;

        COMMIT;

        SET p_resultado = CONCAT('OK: publicacion ', p_idPublicacion, ' y dependencias eliminadas');
    END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `cambios_editor`
--

/*!50001 DROP VIEW IF EXISTS `cambios_editor`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_uca1400_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `cambios_editor` AS select `f`.`responsable` AS `responsable`,`f`.`identificador` AS `identificador`,`f`.`comentario` AS `comentario`,`f`.`fechaCambio` AS `fechaCambio` from ((select `ac`.`responsable` AS `responsable`,`ac`.`identificador` AS `identificador`,`ac`.`comentario` AS `comentario`,`ac`.`fechaCambio` AS `fechaCambio` from `a_controlcambios` `ac` where `ac`.`accion` = 35) union (select `arce`.`autor` AS `responsable`,`arce`.`id` AS `identificador`,`arce`.`comentario` AS `comentario`,`arce`.`fecha` AS `fechaCambio` from `a_registro_cambios_editor` `arce`)) `f` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cambios_fuente`
--

/*!50001 DROP VIEW IF EXISTS `cambios_fuente`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_uca1400_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `cambios_fuente` AS select `f`.`responsable` AS `responsable`,`f`.`identificador` AS `identificador`,`f`.`comentario` AS `comentario`,`f`.`fechaCambio` AS `fechaCambio` from ((select `ac`.`responsable` AS `responsable`,`ac`.`identificador` AS `identificador`,`ac`.`comentario` AS `comentario`,`ac`.`fechaCambio` AS `fechaCambio` from `a_controlcambios` `ac` where `ac`.`accion` like '3%' and `ac`.`accion` <> 35) union (select `arcf`.`autor` AS `responsable`,`arcf`.`id` AS `identificador`,`arcf`.`comentario` AS `comentario`,`arcf`.`fecha` AS `fechaCambio` from `a_registro_cambios_fuente` `arcf`)) `f` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cambios_publicacion`
--

/*!50001 DROP VIEW IF EXISTS `cambios_publicacion`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_uca1400_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `cambios_publicacion` AS select `f`.`responsable` AS `responsable`,`f`.`identificador` AS `identificador`,`f`.`comentario` AS `comentario`,`f`.`fechaCambio` AS `fechaCambio` from ((select `ac`.`responsable` AS `responsable`,`ac`.`identificador` AS `identificador`,`ac`.`comentario` AS `comentario`,`ac`.`fechaCambio` AS `fechaCambio` from `a_controlcambios` `ac` where `ac`.`accion` like '2%') union (select `arcp`.`autor` AS `responsable`,`arcp`.`id` AS `identificador`,`arcp`.`comentario` AS `comentario`,`arcp`.`fecha` AS `fechaCambio` from `a_registro_cambios_publicacion` `arcp`)) `f` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `eliminados_scholar`
--

/*!50001 DROP VIEW IF EXISTS `eliminados_scholar`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `eliminados_scholar` AS select `b`.`nombre` AS `biblioteca`,`e`.`fechaEliminacion` AS `fechaEliminacion`,`i`.`idInvestigador` AS `idInvestigador`,`i`.`nombre` AS `nombre`,`i`.`apellidos` AS `apellidos`,`i`.`docuIden` AS `docuIden`,`i`.`email` AS `email`,`i`.`idCategoria` AS `idCategoria`,`i`.`idArea` AS `idArea`,`i`.`fechaContratacion` AS `fechaContratacion`,`i`.`idDepartamento` AS `idDepartamento`,`i`.`idCentro` AS `idCentro`,`i`.`nacionalidad` AS `nacionalidad`,`i`.`sexo` AS `sexo`,`i`.`fechaNacimiento` AS `fechaNacimiento`,`i`.`fechaNombramiento` AS `fechaNombramiento`,`i`.`perfilPublico` AS `perfilPublico`,`i`.`fechaActualizacion` AS `fechaActualizacion` from ((((`rankings`.`erroneos` `e` join `prisma`.`i_investigador` `i` on(`i`.`idInvestigador` = `e`.`id`)) left join `prisma`.`i_centro` on(`prisma`.`i_centro`.`idCentro` = `i`.`idCentro`)) left join `prisma`.`i_biblioteca` `b` on(`b`.`idBiblioteca` = `prisma`.`i_centro`.`idBiblioteca`)) left join `prisma`.`i_grupo_investigador` `gi` on(`gi`.`idInvestigador` = `i`.`idInvestigador`)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `i_investigador_activo`
--

/*!50001 DROP VIEW IF EXISTS `i_investigador_activo`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_uca1400_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `i_investigador_activo` AS select `i_investigador`.`idInvestigador` AS `idInvestigador`,`i_investigador`.`nombre` AS `nombre`,`i_investigador`.`apellidos` AS `apellidos`,`i_investigador`.`docuIden` AS `docuIden`,`i_investigador`.`email` AS `email`,`i_investigador`.`idCategoria` AS `idCategoria`,`i_investigador`.`fechaNombramiento` AS `fechaNombramiento`,`i_investigador`.`idArea` AS `idArea`,`i_investigador`.`fechaContratacion` AS `fechaContratacion`,`i_investigador`.`idDepartamento` AS `idDepartamento`,`i_investigador`.`idCentro` AS `idCentro`,`i_investigador`.`idCentroCenso` AS `idCentroCenso`,`i_investigador`.`sexo` AS `sexo`,`i_investigador`.`resumen` AS `resumen`,`i_investigador`.`nacionalidad` AS `nacionalidad`,`i_investigador`.`fechaNacimiento` AS `fechaNacimiento`,`i_investigador`.`perfilPublico` AS `perfilPublico`,`i_investigador`.`fechaActualizacion` AS `fechaActualizacion` from `i_investigador` where `i_investigador`.`idInvestigador` in (select distinct `i_fecha_cese`.`idInvestigador` from `i_fecha_cese`) is false */
/*!50002 WITH CASCADED CHECK OPTION */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `investigador_biblioteca`
--

/*!50001 DROP VIEW IF EXISTS `investigador_biblioteca`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=MERGE */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `investigador_biblioteca` AS select `i`.`idInvestigador` AS `idInvestigador`,`b`.`nombre` AS `biblioteca` from ((`i_investigador` `i` join `i_centro` `c` on(`c`.`idCentro` = `i`.`idCentro`)) join `i_biblioteca` `b` on(`b`.`idBiblioteca` = `c`.`idBiblioteca`)) order by `i`.`idInvestigador` */
/*!50002 WITH CASCADED CHECK OPTION */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `investigador_edad`
--

/*!50001 DROP VIEW IF EXISTS `investigador_edad`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `investigador_edad` AS select `i`.`idInvestigador` AS `id`,`i`.`apellidos` AS `apellidos`,`i`.`nombre` AS `nombre`,ifnull(timestampdiff(YEAR,`i`.`fechaNacimiento`,curdate()),'-') AS `edad`,`i`.`fechaNacimiento` AS `fechaNacimiento`,`i`.`email` AS `email` from `i_investigador` `i` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `publicacionesXcentro`
--

/*!50001 DROP VIEW IF EXISTS `publicacionesXcentro`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `publicacionesXcentro` AS select distinct `p`.`idPublicacion` AS `idPublicacion`,`p`.`tipo` AS `tipo`,`p`.`titulo` AS `titulo`,`p`.`agno` AS `agno`,`p`.`idFuente` AS `idFuente`,`p`.`origen` AS `origen`,`p`.`validado` AS `validado`,`p`.`fechaActualizacion` AS `fechaActualizacion`,`p`.`eliminado` AS `eliminado`,`i`.`idCentro` AS `idCentro` from ((`p_publicacion` `p` join `p_autor` `a` on(`a`.`idPublicacion` = `p`.`idPublicacion`)) join `i_investigador_activo` `i` on(`i`.`idInvestigador` = `a`.`idInvestigador`)) where `p`.`tipo` <> 'Tesis' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `r_pbi`
--

/*!50001 DROP VIEW IF EXISTS `r_pbi`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_uca1400_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `r_pbi` AS select `iia`.`nombre` AS `NOMBRE`,`iia`.`apellidos` AS `APELLIDOS`,`id`.`idDepartamento` AS `ID_DEPARTAMENTO`,`id`.`nombre` AS `DEPARTAMENTO`,`ig`.`idGrupo` AS `ID_GRUPO`,`ig`.`nombre` AS `GRUPO`,`ic`.`idCentro` AS `ID_CENTRO`,`ic`.`nombre` AS `CENTRO`,`ii`.`idInstituto` AS `ID_INSTITUTO`,`ii`.`nombre` AS `INSTITUTO`,`iia`.`idInvestigador` AS `Id_Prisma`,max(case when `iii`.`tipo` = 'scopus' then `iii`.`valor` end) AS `Id_Scopus`,max(case when `iii`.`tipo` = 'researcherId' then `iii`.`valor` end) AS `Id_Wos`,max(case when `iii`.`tipo` = 'openalex' then `iii`.`valor` end) AS `Id_Openalex`,max(case when `mi`.`tipo` = 'num_pub' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Pubs_Total_Scopus`,max(case when `mi`.`tipo` = 't_citas' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Total_Citas_Total_Scopus`,max(case when `mi`.`tipo` = 'indice_h' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `Indice_H_Total_Scopus`,max(case when `mi`.`tipo` = 'num_pub_10' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Pubs_10_Scopus`,max(case when `mi`.`tipo` = 't_citas_10' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Total_Citas_10_Scopus`,max(case when `mi`.`tipo` = 'indice_h_10' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `Indice_H_10_Scopus`,max(case when `mi`.`tipo` = 'num_pub_5' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Pubs_5_Scopus`,max(case when `mi`.`tipo` = 't_citas_5' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Total_Citas_5_Scopus`,max(case when `mi`.`tipo` = 'indice_h_5' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `Indice_H_5_Scopus`,max(case when `mi`.`tipo` = 'num_pub_3' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Pubs_3_Scopus`,max(case when `mi`.`tipo` = 't_citas_3' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `N_Total_Citas_3_Scopus`,max(case when `mi`.`tipo` = 'indice_h_3' and `mi`.`basedatos` = 'scopus' then `mi`.`valor` end) AS `Indice_H_3_Scopus`,max(case when `mi`.`tipo` = 'num_pub' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Pubs_Total_Wos`,max(case when `mi`.`tipo` = 't_citas' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Total_Citas_Total_Wos`,max(case when `mi`.`tipo` = 'indice_h' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `Indice_H_Total_Wos`,max(case when `mi`.`tipo` = 'num_pub_10' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Pubs_10_Wos`,max(case when `mi`.`tipo` = 't_citas_10' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Total_Citas_10_Wos`,max(case when `mi`.`tipo` = 'indice_h_10' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `Indice_H_10_Wos`,max(case when `mi`.`tipo` = 'num_pub_5' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Pubs_5_Wos`,max(case when `mi`.`tipo` = 't_citas_5' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Total_Citas_5_Wos`,max(case when `mi`.`tipo` = 'indice_h_5' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `Indice_H_5_Wos`,max(case when `mi`.`tipo` = 'num_pub_3' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Pubs_3_Wos`,max(case when `mi`.`tipo` = 't_citas_3' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `N_Total_Citas_3_Wos`,max(case when `mi`.`tipo` = 'indice_h_3' and `mi`.`basedatos` = 'wos' then `mi`.`valor` end) AS `Indice_H_3_Wos`,`ir`.`nombre` AS `Rama` from ((((((((((`i_investigador_activo` `iia` left join `i_departamento` `id` on(`id`.`idDepartamento` = `iia`.`idDepartamento`)) left join `i_rama_us` `iru` on(`iru`.`idDepartamento` = `id`.`idDepartamento`)) left join `i_rama` `ir` on(`ir`.`idRama` = `iru`.`idRama`)) left join `i_grupo_investigador` `igi` on(`igi`.`idInvestigador` = `iia`.`idInvestigador`)) left join `i_grupo` `ig` on(`ig`.`idGrupo` = `igi`.`idGrupo`)) left join `i_centro` `ic` on(`ic`.`idCentro` = `iia`.`idCentro`)) left join `i_miembro_instituto` `imi` on(`imi`.`idInvestigador` = `iia`.`idInvestigador`)) left join `i_instituto` `ii` on(`ii`.`idInstituto` = `imi`.`idInstituto`)) left join `i_identificador_investigador` `iii` on(`iii`.`idInvestigador` = `iia`.`idInvestigador`)) left join (select `m_informes`.`idMetrica` AS `idMetrica`,`m_informes`.`ambito` AS `ambito`,`m_informes`.`identificador` AS `identificador`,`m_informes`.`basedatos` AS `basedatos`,`m_informes`.`tipo` AS `tipo`,`m_informes`.`valor` AS `valor`,`m_informes`.`fechaActualizacion` AS `fechaActualizacion`,`m_informes`.`identificadorInt` AS `identificadorInt` from `m_informes` where `m_informes`.`ambito` = 'investigador') `mi` on(`mi`.`identificadorInt` = `iia`.`idInvestigador`)) group by `iia`.`idInvestigador` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ranking_scholar`
--

/*!50001 DROP VIEW IF EXISTS `ranking_scholar`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ranking_scholar` AS select distinct `i_investigador_activo`.`idInvestigador` AS `Id`,`sch`.`valor` AS `idScholar`,concat(`i_investigador_activo`.`nombre`,' ',`i_investigador_activo`.`apellidos`) AS `nombre`,`i_investigador_activo`.`idDepartamento` AS `idDepartamento`,`i_investigador_activo`.`idArea` AS `idArea`,`i_rama`.`padre` AS `idRamaAneca`,`ru`.`idRama` AS `idRama`,`m`.`nDocs` AS `nDocs`,`m`.`citasTotal` AS `citasTotal`,`m`.`indiceHTotal` AS `indiceHTotal`,`m`.`indiceI10Total` AS `indiceI10Total`,`m`.`citasHace5` AS `citasHace5`,`m`.`indiceHHace5` AS `indiceHHace5`,`m`.`indiceI10Hace5` AS `indiceI10Hace5` from (((((`i_investigador_activo` join `i_identificador_investigador` `sch` on(`i_investigador_activo`.`idInvestigador` = `sch`.`idInvestigador`)) join `m_scholar` `m` on(`sch`.`valor` = `m`.`idScholar`)) join `i_area` on(`i_area`.`idArea` = `i_investigador_activo`.`idArea`)) join `i_rama` on(`i_rama`.`idRama` = `i_area`.`idRama`)) join `i_rama_us` `ru` on(`ru`.`idDepartamento` = `i_investigador_activo`.`idDepartamento` and `ru`.`idArea` = `i_investigador_activo`.`idArea`)) where `sch`.`tipo` = 'scholar' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ranking_scholar_mejorado`
--

/*!50001 DROP VIEW IF EXISTS `ranking_scholar_mejorado`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ranking_scholar_mejorado` AS select distinct `i_investigador_activo`.`idInvestigador` AS `Id`,`sch`.`valor` AS `idScholar`,concat(`i_investigador_activo`.`nombre`,' ',`i_investigador_activo`.`apellidos`) AS `nombre`,`i_investigador_activo`.`idDepartamento` AS `idDepartamento`,`i_investigador_activo`.`idArea` AS `idArea`,`i_rama`.`padre` AS `idRamaAneca`,ifnull(`ru`.`idRama`,0) AS `idRama`,`m`.`nDocs` AS `nDocs`,`m`.`citasTotal` AS `citasTotal`,`m`.`indiceHTotal` AS `indiceHTotal`,`m`.`indiceI10Total` AS `indiceI10Total`,`m`.`citasHace5` AS `citasHace5`,`m`.`indiceHHace5` AS `indiceHHace5`,`m`.`indiceI10Hace5` AS `indiceI10Hace5` from (((((`i_investigador_activo` join `i_identificador_investigador` `sch` on(`i_investigador_activo`.`idInvestigador` = `sch`.`idInvestigador`)) join `m_scholar` `m` on(`sch`.`valor` = `m`.`idScholar`)) left join `i_area` on(`i_area`.`idArea` = `i_investigador_activo`.`idArea`)) left join `i_rama` on(`i_rama`.`idRama` = `i_area`.`idRama`)) left join `i_rama_us` `ru` on(`ru`.`idDepartamento` = `i_investigador_activo`.`idDepartamento` and `ru`.`idArea` = `i_investigador_activo`.`idArea`)) where `sch`.`tipo` = 'scholar' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ranking_scopus`
--

/*!50001 DROP VIEW IF EXISTS `ranking_scopus`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ranking_scopus` AS select `i`.`idInvestigador` AS `idInves`,`s`.`valor` AS `idScopus`,concat(`i`.`nombre`,' ',`i`.`apellidos`) AS `nombre`,`i`.`idDepartamento` AS `idDepartamento`,`i`.`idArea` AS `idArea`,ifnull(`us`.`idRama`,0) AS `idRama`,`aneca`.`padre` AS `idRamaAneca`,`m`.`citation_count` AS `citation_count`,`m`.`cited_by_count` AS `cited_by_count`,`m`.`coauthor_count` AS `coauthor_count`,`m`.`document_count` AS `document_count`,`m`.`h_index` AS `h_index` from (((((`i_investigador_activo` `i` join `i_identificador_investigador` `s` on(`s`.`idInvestigador` = `i`.`idInvestigador` and `s`.`tipo` = 'scopus')) join `m_scopus` `m` on(`m`.`idScopus` = `s`.`valor`)) left join `i_area` `a` on(`a`.`idArea` = `i`.`idArea`)) left join `i_rama` `aneca` on(`aneca`.`idRama` = `a`.`idRama`)) left join `i_rama_us` `us` on(`us`.`idArea` = `i`.`idArea` and `us`.`idDepartamento` = `i`.`idDepartamento`)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `tesis_fecha_lectura_mal`
--

/*!50001 DROP VIEW IF EXISTS `tesis_fecha_lectura_mal`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `tesis_fecha_lectura_mal` AS select concat('https://bibliometria.us.es/prisma/publicacion/',`p_dato_publicacion`.`idPublicacion`) AS `URL`,`p_dato_publicacion`.`valor` AS `fecha_fectura` from `p_dato_publicacion` where `p_dato_publicacion`.`tipo` = 'fecha_lectura' and `p_dato_publicacion`.`idPublicacion` in (select `p_publicacion`.`idPublicacion` from `p_publicacion` where `p_publicacion`.`tipo` = 'Tesis' and `p_publicacion`.`eliminado` = 0) and !(`p_dato_publicacion`.`valor` regexp '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]') */
/*!50002 WITH CASCADED CHECK OPTION */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2026-09-22 11:24:22
