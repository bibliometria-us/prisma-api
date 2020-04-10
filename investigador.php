<?php 
require_once 'BaseDatos.php';
$bd = new BaseDatos();
$ident = 0;
if (isset($_GET['idus']) && isset($_GET['idus']) != ''){
    $ident = $_GET['idus'];
} else {
    header("HTTP/1.0 400 Bad Request");
    exit();
}
$metodo = $_SERVER['REQUEST_METHOD'];
//echo $_SERVER['REQUEST_URI'];
switch ($metodo) {
    case 'GET':
        ob_start();
        $identificador = $bd->obtenerPorIdentificador('idus', $ident);
        $investigador = $bd->obtenerInvestigador($identificador);
        $identificadores = $bd->obtenerIdentificadoresInves($identificador);
        echo "<investigador>";
        if (gettype($investigador) == 'object') {
            echo "<nombre_administrativo>" . $investigador->apellidos . ", ". $investigador->nombre . "</nombre_administrativo>";
            echo "<prisma>" . $identificador . "</prisma>";
            if (array_key_exists('orcid', $identificadores)) {
                echo "<orcid>" . $identificadores['orcid'] . "</orcid>";
            } else {
	        echo "<orcid></orcid>";
	    }
            if (array_key_exists('researcherid', $identificadores)) {
                echo "<researcherid>" . $identificadores['researcherid'] . "</researcherid>";
            } else {
	        echo "<researcherid></researcherid>";
	    }
            if (array_key_exists('sisius', $identificadores)) {
                echo "<sisius>" . $identificadores['sisius'] . "</sisius>";
            } else {
	        echo "<sisius></sisus>";
	    }
            if (array_key_exists('scopus', $identificadores)) {
                echo "<scopus>" . $identificadores['scopus'] . "</scopus>";
            } else {
	        echo "<scopus></scopus>";
	    }
            if (array_key_exists('scholar', $identificadores)) {
                echo "<scholar>" . $identificadores['scholar'] . "</scholar>";
            } else {
	        echo "<scholar></scholar>";
	    }
            if (array_key_exists('dialnet', $identificadores)) {
                echo "<dialnet>" . $identificadores['dialnet'] . "</dialnet>";
            } else {
	        echo "<dialnet></dialnet>";
	    }
            
            echo "<area_conocimiento>" . $investigador->area . "</area_conocimiento>";
            
            echo "<departamento>" . $investigador->departamento . "</departamento>";
            
            echo "<email>" . $investigador->email . "</email>";
            if (array_key_exists('idus', $identificadores)) {
                echo "<idus>" . $identificadores['idus'] . "</idus>";
            } 
        }
        echo "</investigador>";
        http_response_code(200);
        header('Content-Type: application/xml; charset=utf-8');
        ob_end_flush();
        break;
    default:
        header("HTTP/1.0 404 Not Found");
}

?>
