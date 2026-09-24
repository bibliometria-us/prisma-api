/*M!999999\- enable the sandbox mode */ 
-- MariaDB dump 10.19-11.7.2-MariaDB, for Win64 (AMD64)
--
-- Host: localhost    Database: prisma_proyectos
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
-- Table structure for table `proyecto`
--

DROP TABLE IF EXISTS `proyecto`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `proyecto` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tipo` varchar(100) NOT NULL,
  `nombre` varchar(600) NOT NULL,
  `referencia` varchar(50) DEFAULT NULL,
  `organica` varchar(12) DEFAULT NULL,
  `inicio` date NOT NULL,
  `fin` date DEFAULT NULL,
  `ambito` varchar(25) NOT NULL,
  `concedido` double(10,2) DEFAULT NULL,
  `solicitado` double(10,2) DEFAULT NULL,
  `prog_financiador` varchar(100) DEFAULT NULL,
  `entidad_financiadora` varchar(200) DEFAULT NULL COMMENT 'incluidas empresas financiadoras',
  `competitivo` tinyint(1) NOT NULL DEFAULT 0,
  `visible` tinyint(1) NOT NULL DEFAULT 1,
  `sisius_id` int(11) DEFAULT NULL,
  `creado` timestamp NULL DEFAULT current_timestamp(),
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `nombre` (`nombre`(191)),
  KEY `referencia` (`referencia`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=18400 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `proyecto_20260529`
--

DROP TABLE IF EXISTS `proyecto_20260529`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `proyecto_20260529` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tipo` varchar(100) NOT NULL,
  `nombre` varchar(600) NOT NULL,
  `referencia` varchar(50) DEFAULT NULL,
  `organica` varchar(12) DEFAULT NULL,
  `inicio` date NOT NULL,
  `fin` date DEFAULT NULL,
  `ambito` varchar(25) NOT NULL,
  `concedido` double(10,2) DEFAULT NULL,
  `solicitado` double(10,2) DEFAULT NULL,
  `prog_financiador` varchar(100) DEFAULT NULL,
  `entidad_financiadora` varchar(200) DEFAULT NULL COMMENT 'incluidas empresas financiadoras',
  `competitivo` tinyint(1) NOT NULL DEFAULT 0,
  `visible` tinyint(1) NOT NULL DEFAULT 1,
  `sisius_id` int(11) DEFAULT NULL,
  `creado` timestamp NULL DEFAULT NULL,
  `actualizado` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `nombre` (`nombre`(191)) USING BTREE,
  KEY `referencia` (`referencia`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=18315 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `proyecto_ignorado`
--

DROP TABLE IF EXISTS `proyecto_ignorado`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `proyecto_ignorado` (
  `sisius_id` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`sisius_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `proyecto_miembro`
--

DROP TABLE IF EXISTS `proyecto_miembro`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `proyecto_miembro` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `proyecto_id` bigint(20) NOT NULL,
  `firma` varchar(250) NOT NULL,
  `rol` varchar(50) NOT NULL,
  `investigador_id` int(10) DEFAULT NULL,
  `fecha_alta` date DEFAULT NULL,
  `fecha_baja` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `proyecto_miembro_proyecto_id_firma_rol` (`proyecto_id`,`firma`,`rol`) USING BTREE,
  KEY `proyecto_id` (`proyecto_id`),
  KEY `investigador_id` (`investigador_id`),
  KEY `proyecto_miembro_proyecto_id_IDX` (`proyecto_id`,`firma`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=89059 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping routines for database 'prisma_proyectos'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

-- Dump completed on 2026-09-22 11:57:52
