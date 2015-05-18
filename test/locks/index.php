<?php
set_include_path(
    get_include_path()
    . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
    . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/mongo'
);

require_once 'tripod.inc.php';
require_once 'Tripod.class.phpquire_once 'Logger.php';


$config = json_decode(file_get_contents('tripod-config.json'), true);
Config::setConfig($config);

$tripod = new MongoTripod('CBD_nodes', 'life', array('retriesToGetLock' => 5000));
MongoTripod::$logger = Logger::getLogger();


$g = $tripod->describeResource("http://life.ac.uk/");

$oldGraph = new ExtendedGraph();
$oldGraph->from_graph($g);

//echo $g->to_rdfxml();

$g->add_literal_triple("http://life.ac.uk/", 'http://purl.org/dc/terms/text', 'current time in micro-seconds : ' .microtime());

try{
    $tripod->saveChanges($oldGraph, $g);
}
catch (Exception $e){
    echo $e->getTraceAsString();
}




