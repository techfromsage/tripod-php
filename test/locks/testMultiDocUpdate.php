<?php
/**
 * Tests updating multiple subjects in a transaction.
 */

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

$resources =array(
    "http://life.ac.uk/",
    "http://life.ac.uk/centers/center_for_advanced_technology",
    "http://life.ac.uk/colleges/college_of_modern_languages",
    "http://life.ac.uk/departments/finance",
    "http://life.ac.uk/divisions/ipd"
);

$g = $tripod->describeResources($resources);

$oldGraph = new ExtendedGraph();
$oldGraph->from_graph($g);

foreach($resources as $r){
    $g->add_literal_triple($r, 'http://purl.org/dc/terms/text', 'Time in micro-seconds : ' .microtime());
    $g->add_literal_triple($r, 'http://purl.org/dc/terms/description', 'Testing concurrent multi doc update in Tripod');
}

try{
    $tripod->saveChanges($oldGraph, $g);
}
catch (Exception $e){
    echo $e->getTraceAsString();
}




