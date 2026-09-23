/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-11.7.2-MariaDB, for Win64 (AMD64)
--
-- Host: localhost    Database: prisma_cvn
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
-- Table structure for table `paises`
--

DROP TABLE IF EXISTS `paises`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `paises` (
  `identificador` int(3) NOT NULL,
  `pais` varchar(38) DEFAULT NULL,
  PRIMARY KEY (`identificador`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `peticion`
--

DROP TABLE IF EXISTS `peticion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `peticion` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `solicitante` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL COMMENT 'UVUS del solicitante',
  `solicitud` timestamp NOT NULL DEFAULT current_timestamp() COMMENT 'Fecha de solicitud del CV',
  `entrega` timestamp NULL DEFAULT NULL COMMENT 'Fecha de entrega del CV o de la información del error',
  `tipo` varchar(15) NOT NULL,
  `investigador_id` bigint(20) NOT NULL,
  `cvn` text NOT NULL COMMENT 'Datos del CVN a generar',
  `respuesta` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL COMMENT 'Respuesta dada por la FECYT\r\nreturnCode: Puede tener dos valores: \r\n-00: La generación del CVN-PDF ha sido correcta, el fichero devuelto es un PDF.\r\n-01: Ha habido errores en la generación del CVN-PDF. El fichero devuelto es un XML que NO debe ser devuelto al investigador. \r\n-02: Se ha sobrepasado el número máximo de páginas permitido por FECYT en la generación del CVA. El fichero devuelto es un CVA-PDF no valido.',
  `reintentos` int(3) NOT NULL DEFAULT 5,
  PRIMARY KEY (`id`),
  KEY `solicitantes` (`solicitante`),
  KEY `entrega` (`entrega`),
  KEY `reintentos` (`reintentos`)
) ENGINE=InnoDB AUTO_INCREMENT=14623 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `peticion_preproduccion`
--

DROP TABLE IF EXISTS `peticion_preproduccion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `peticion_preproduccion` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `solicitante` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL COMMENT 'UVUS del solicitante',
  `solicitud` timestamp NOT NULL DEFAULT current_timestamp() COMMENT 'Fecha de solicitud del CV',
  `entrega` timestamp NULL DEFAULT NULL COMMENT 'Fecha de entrega del CV o de la información del error',
  `tipo` varchar(15) NOT NULL,
  `investigador_id` bigint(20) NOT NULL,
  `cvn` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL COMMENT 'Datos del CVN a generar',
  `respuesta` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL COMMENT 'Respuesta dada por la FECYT',
  PRIMARY KEY (`id`),
  KEY `solicitantes` (`solicitante`)
) ENGINE=InnoDB AUTO_INCREMENT=56 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prog_doctorado`
--

DROP TABLE IF EXISTS `prog_doctorado`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `prog_doctorado` (
  `codigo` int(11) NOT NULL,
  `nombre` varchar(350) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `delegado` int(11) DEFAULT NULL,
  PRIMARY KEY (`codigo`),
  KEY `nombre` (`nombre`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `unesco`
--

DROP TABLE IF EXISTS `unesco`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `unesco` (
  `cod1` varchar(2) NOT NULL,
  `cod2` varchar(2) NOT NULL,
  `cod3` varchar(2) NOT NULL,
  `spa` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci NOT NULL,
  `eng` varchar(150) NOT NULL,
  PRIMARY KEY (`cod1`,`cod2`,`cod3`),
  KEY `spa` (`spa`),
  KEY `eng` (`eng`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'prisma_cvn'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2026-09-22 11:57:47
