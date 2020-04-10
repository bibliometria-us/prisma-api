<?php 

require_once 'BaseDatos.php';
$bd = new BaseDatos();

$metodo = $_SERVER['REQUEST_METHOD'];
//echo $_SERVER['REQUEST_URI'];
$empezar = 0;
if (isset($_GET['empezar']) && isset($_GET['empezar']) != ''){
    $empezar = $_GET['empezar'];
}
switch ($metodo) {
    case 'GET':
        ob_start();
        $investigadores = $bd->obtenerIdentificadoresInvestigadores();
        unset($bd);
        echo "<investigadores>";
        foreach ($investigadores as $investigador) {
            echo "<investigador>";
            echo "<nombre_administrativo>" . $investigador['nombre'] ."</nombre_administrativo>";
            echo "<prisma>" . $investigador['prisma'] ."</prisma>";
            echo "<orcid>" . $investigador['orcid'] ."</orcid>";
            echo "<researcherid>" . $investigador['researcherid'] ."</researcherid>";
            echo "<scopus>" . $investigador['scopus'] ."</scopus>";
            echo "<scholar>" . $investigador['scholar'] ."</scholar>";
            echo "<dialnet>" . $investigador['dialnet'] ."</dialnet>";
            echo "<sisius>" . $investigador['sisius'] ."</sisius>";
            echo "<idus>" . $investigador['idus'] ."</idus>";
            echo "</investigador>";
        }
        echo "</investigadores>";
        http_response_code(200);
        header('Content-Type: application/xml; charset=utf-8');
        //sleep(10);
        ob_end_flush();
        break;
    default:
        header("HTTP/1.0 404 Not Found");
}

?>
