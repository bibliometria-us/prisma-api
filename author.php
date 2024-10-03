<?php 
require_once 'BaseDatos.php';
$bd = new BaseDatos();
$ident = 0;
if (isset($_GET['idus']) && $_GET['idus'] != ''){
    $ident = $_GET['idus'];
    $identificador = $bd->obtenerPorIdentificador('idus', $ident);
} else if (isset($_GET['id']) && $_GET['id'] != '' && is_numeric($_GET['id'])){
    $identificador = $_GET['id'];
} else {
    http_response_code(400);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(array('error' => 'Bad Request'));
    exit();
}
$metodo = $_SERVER['REQUEST_METHOD'];
//echo $_SERVER['REQUEST_URI'];
switch ($metodo) {
    case 'GET':
        $investigador = $bd->obtenerInvestigador($identificador);
        $identificadores = $bd->obtenerIdentificadoresInves($identificador);
        
        $response = array(
            'name' => $investigador->apellidos . ", ". $investigador->nombre,
            'category' => $investigador->categoria,
            'prisma' => $identificador,
            'orcid' => array_key_exists('orcid', $identificadores) ? $identificadores['orcid'] : null,
            'researcherid' => array_key_exists('researcherid', $identificadores) ? $identificadores['researcherid'] : null,
            'sisius' => array_key_exists('sisius', $identificadores) ? $identificadores['sisius'] : null,
            'scopus' => array_key_exists('scopus', $identificadores) ? $identificadores['scopus'] : null,
            'scholar' => array_key_exists('scholar', $identificadores) ? $identificadores['scholar'] : null,
            'dialnet' => array_key_exists('dialnet', $identificadores) ? $identificadores['dialnet'] : null,
            'area' => $investigador->area,
            'department' => $investigador->departamento,
            'email' => $investigador->email,
            'idus' => array_key_exists('idus', $identificadores) ? $identificadores['idus'] : null
        );
        
        http_response_code(200);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($response);
        break;
    default:
        http_response_code(404);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode(array('error' => 'Not Found'));
}

