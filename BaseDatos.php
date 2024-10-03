<?php

require_once 'configuracion.php';

class BaseDatos {
	public $conexion = null;
	
	public function __construct($editor=false)
	{	
		if ($editor)
			$this->conexion = new mysqli(SERVIDOR, USUARIOED, PASSWORDED, BASEDATOS);
		else 
			$this->conexion = new mysqli(SERVIDOR, USUARIO, PASSWORD, BASEDATOS);
		if ($this->conexion->connect_errno) {
			echo "Lo sentimos, este sitio web está experimentando problemas.";
			echo "Error: Fallo al conectarse a MySQL debido a: \n";
			exit();
		}
		$this->conexion->set_charset("utf8");
	}
	
	public function __destruct() {
		if ($this->conexion != null)
			$this->conexion->close();
	}
	
	
	/**
	 * Función que obtiene la información de un investigador dado su identificador en nuestra base de datos
	 * @param int $id
	 * @return resultado de la base de datos
	 */
	public function obtenerInvestigador($id) {
	    $id = $this->conexion->real_escape_string($id);
	    $sql = "SELECT i.idInvestigador as idInves, i.nombre, apellidos, email, i.idDepartamento, d.nombre AS departamento, gi.idGrupo, g.nombre AS grupo, i.idArea, a.nombre AS area,
			    i.idCentro, b.nombre AS centro, i.idCategoria, CASE WHEN i.sexo = 0 THEN c.femenino ELSE c.nombre END AS categoria, perfilPublico FROM i_investigador i INNER JOIN i_departamento d ON i.idDepartamento = d.idDepartamento 
				LEFT JOIN i_grupo_investigador gi ON gi.idInvestigador = i.idInvestigador
				LEFT JOIN i_grupo g ON gi.idGrupo = g.idGrupo 
				LEFT JOIN i_area a ON i.idArea = a.idArea 
				LEFT JOIN i_centro b ON i.idCentro = b.idCentro
				LEFT JOIN i_categoria c ON i.idCategoria = c.idCategoria WHERE i.idInvestigador = $id and i.idInvestigador not in (SELECT idInvestigador FROM i_fecha_cese WHERE idMotivo != 'EXCL')";
	    $consulta = $this->conexion->query($sql);
	    //echo $sql;
	    if ($consulta === false || $consulta->num_rows == 0)
	        return 0;
	        else
	            return $consulta->fetch_object();
	}
	
    public function obtenerPorIdentificador($tipo, $id) {
        $sql = "SELECT idInvestigador as id FROM i_identificador_investigador WHERE tipo = ? AND valor = ?";
        $consulta = $this->conexion->prepare($sql);
        $consulta->bind_param('ss', $tipo, $id);
        $consulta->bind_result($identificador);
        if (!$consulta->execute() || !$consulta->fetch())
            $identificador = 0;
        $consulta->close();
	    return $identificador;
	}
	
	public function obtenerIdentificadoresInves($investigador) {
	    $investigador = $this->conexion->real_escape_string($investigador);
	    $sql = "SELECT tipo, GROUP_CONCAT(valor SEPARATOR ';') as valor FROM `i_identificador_investigador` where idInvestigador = $investigador AND eliminado = 0 AND idInvestigador not in (SELECT idInvestigador FROM i_fecha_cese WHERE idMotivo != 'EXCL') GROUP BY tipo";
	    $resultado = $this->conexion->query($sql);
	    if ($resultado === False || $resultado->num_rows <= 0) {
	        return [];
	    } else {
	        $salida = [];
	        while ($fila = $resultado->fetch_object()) {
	            $salida[$fila->tipo] = $fila->valor;
	        }
	        return $salida;
	    }
	}
	
		
	public function obtenerIdentificadoresInvestigadores() {
	    $sql = "SELECT concat(i.apellidos, ', ', i.nombre) as nombre, c.* 
				FROM i_investigador i 
				LEFT JOIN (
					SELECT ii.idInvestigador prisma, 
						   GROUP_CONCAT(if(ii.tipo = 'scopus', ii.valor, NULL)) scopus, 
						   GROUP_CONCAT(if(ii.tipo = 'orcid', ii.valor, NULL)) orcid, 
						   GROUP_CONCAT(if(ii.tipo = 'researcherid', ii.valor, NULL)) researcherid, 
						   GROUP_CONCAT(if(ii.tipo = 'idus', ii.valor, NULL)) idus, 
						   GROUP_CONCAT(if(ii.tipo = 'sisius', ii.valor, NULL)) sisius, 
						   GROUP_CONCAT(if(ii.tipo = 'scholar', ii.valor, NULL)) scholar, 
						   GROUP_CONCAT(if(ii.tipo = 'dialnet', ii.valor, NULL)) dialnet 
					FROM i_identificador_investigador ii 
					GROUP BY ii.idInvestigador
				) c ON c.prisma = i.idInvestigador 
				WHERE i.idInvestigador NOT IN (
					SELECT idInvestigador 
					FROM i_fecha_cese 
					WHERE idMotivo != 'EXCL'
				)
				AND c.idus IS NOT NULL;";
	    //echo $sql;
	    $consulta = $this->conexion->query($sql);
	    return $consulta->fetch_all(MYSQLI_ASSOC);
	}

	
/* 	public function obtenerIdentificadoresInvestigadores() {
	    $sql = "SELECT concat(i.apellidos, ', ', i.nombre) as nombre, c.* FROM i_investigador i 
                LEFT JOIN (SELECT ii.idInvestigador prisma, GROUP_CONCAT(if(ii.tipo = 'scopus', ii.valor, NULL)) scopus, 
                            GROUP_CONCAT(if(ii.tipo = 'orcid', ii.valor, NULL)) orcid, 
                            GROUP_CONCAT(if(ii.tipo = 'researcherid', ii.valor, NULL)) researcherid, 
                            GROUP_CONCAT(if(ii.tipo = 'idus', ii.valor, NULL)) idus, 
                            GROUP_CONCAT(if(ii.tipo = 'sisius', ii.valor, NULL)) sisius, 
                            GROUP_CONCAT(if(ii.tipo = 'scholar', ii.valor, NULL)) scholar, 
                            GROUP_CONCAT(if(ii.tipo = 'dialnet', ii.valor, NULL)) dialnet 
                            FROM i_identificador_investigador ii group by ii.idInvestigador) c 
                ON c.prisma = i.idInvestigador WHERE i.idInvestigador not in (SELECT idInvestigador FROM i_fecha_cese WHERE idMotivo != 'EXCL')";
	    //echo $sql;
	    $consulta = $this->conexion->query($sql);
	    return $consulta->fetch_all(MYSQLI_ASSOC);
	} */ 

}
?>
