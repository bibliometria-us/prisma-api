<?php 

require_once 'BaseDatos.php';
$bd = new BaseDatos();

$metodo = $_SERVER['REQUEST_METHOD'];
//echo $_SERVER['REQUEST_URI'];
$empezar = 0;
if (isset($_GET['empezar']) && $_GET['empezar'] != ''){
    $empezar = $_GET['empezar'];
}
switch ($metodo) {
    case 'GET':
        $investigadores = $bd->obtenerIdentificadoresInvestigadores();
        unset($bd);
        
        $response = array();
        foreach ($investigadores as $investigador) {
            $investigadorObj = array(
                'name' => $investigador['nombre'],
                'prisma' => $investigador['prisma'],
                'orcid' => $investigador['orcid'],
                'researcherid' => $investigador['researcherid'],
                'scopus' => $investigador['scopus'],
                'scholar' => $investigador['scholar'],
                'dialnet' => $investigador['dialnet'],
                'sisius' => $investigador['sisius'],
                'idus' => $investigador['idus']
            );
            $response[] = $investigadorObj;
        }
        
        http_response_code(200);
        header('Content-Type: application/json; charset=utf-8');
        echo json_encode($response);
        break;
    default:
        header("HTTP/1.0 404 Not Found");
}


