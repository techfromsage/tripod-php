<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

require_once('./common.inc.php');
require_once('../../src/tripod.inc.php');

$config = json_decode(file_get_contents('../../test/rest-interface/config/tripod-config.json'), true);

\Tripod\Mongo\Config::setConfig($config);
\Tripod\Mongo\Config::getInstance()->setConfig($config);

$tripod = new \Tripod\Mongo\Driver('CBD_config','surrey',array('defaultContext'=>'http://talisaspire.com/'));

require_once '../../src/tripod.inc.php';
$collection = (new MongoDB\Client)->surrey->CBD_config;
$cursor = $collection->find(array(), array('typeMap' => array('root' => 'array', 'document' => 'array', 'array' => 'array')));
foreach($cursor as $doc) {
    $etag = isset($doc['_uts']) ? getEtag($doc['_uts']) : '' ;
    echo '_id.r: '.$doc['_id']['r'] . ' - ' . $etag . "\n";
}


function getEtag($date) {
    $seconds = $date->__toString() / 1000;
    $eTag = str_pad(number_format(($seconds - floor($seconds)), 6), 10, '0', STR_PAD_RIGHT) . ' ' . floor($seconds);
    return $eTag;
}