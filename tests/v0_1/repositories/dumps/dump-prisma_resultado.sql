/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-11.7.2-MariaDB, for Win64 (AMD64)
--
-- Host: localhost    Database: prisma_resultado
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
-- Table structure for table `dato_patente`
--

DROP TABLE IF EXISTS `dato_patente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `dato_patente` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL COMMENT 'url, titulo_alternativo',
  `valor` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `patente_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `dato_patente_id` (`patente_id`),
  CONSTRAINT `dato_patente_id` FOREIGN KEY (`patente_id`) REFERENCES `patente` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=10759 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `inventor_patente`
--

DROP TABLE IF EXISTS `inventor_patente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `inventor_patente` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `patente_id` bigint(20) unsigned NOT NULL,
  `nombre` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `investigador_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `inventor_patente_id` (`patente_id`),
  KEY `inventor_investigador_id` (`investigador_id`),
  CONSTRAINT `inventor_investigador_id` FOREIGN KEY (`investigador_id`) REFERENCES `prisma`.`i_investigador` (`idInvestigador`) ON UPDATE CASCADE,
  CONSTRAINT `inventor_patente_id` FOREIGN KEY (`patente_id`) REFERENCES `patente` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7644 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `materia_cip`
--

DROP TABLE IF EXISTS `materia_cip`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `materia_cip` (
  `codigo` varchar(1) NOT NULL,
  `materia` varchar(248) DEFAULT NULL,
  PRIMARY KEY (`codigo`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `materia_patente`
--

DROP TABLE IF EXISTS `materia_patente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `materia_patente` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `patente_id` bigint(20) unsigned NOT NULL,
  `clasificacion` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `materia` varchar(25) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `edicion` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `materia_patente_id` (`patente_id`),
  CONSTRAINT `materia_patente_id` FOREIGN KEY (`patente_id`) REFERENCES `patente` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=6170 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `patente`
--

DROP TABLE IF EXISTS `patente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `patente` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `tipo` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL DEFAULT 'patente',
  `numero_solicitud` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `fecha_solicitud` date NOT NULL,
  `titulo` varchar(750) CHARACTER SET utf32 COLLATE utf32_spanish_ci NOT NULL,
  `idioma_titulo` varchar(2) CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL DEFAULT 'es',
  `resumen` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci DEFAULT NULL,
  `visible` tinyint(1) NOT NULL DEFAULT 1,
  `fecha_incorporacion` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `fecha_actualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `numero_solicitud` (`numero_solicitud`),
  KEY `titulo` (`titulo`(191))
) ENGINE=InnoDB AUTO_INCREMENT=1839 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `titular_patente`
--

DROP TABLE IF EXISTS `titular_patente`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `titular_patente` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `patente_id` bigint(20) unsigned NOT NULL,
  `nombre` text CHARACTER SET utf8mb3 COLLATE utf8mb3_spanish_ci NOT NULL,
  `porcentaje` decimal(5,2) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `patente_id` (`patente_id`),
  CONSTRAINT `titular_patente_id` FOREIGN KEY (`patente_id`) REFERENCES `patente` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2825 DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'prisma_resultado'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2026-09-22 11:24:25
