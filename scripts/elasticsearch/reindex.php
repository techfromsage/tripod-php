<?php
set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/MongoTripod.class.php';
require_once 'mongo/MongoTripodQueue.class.php';

if ($argc!=4 && $argc!=5 && $argc!=6)
{
	echo "usage: ./reindex.php tripodConfig.json dbName collection [type] [_id]\n"; //todo - discover dbName and collection from config?
	die();
}
array_shift($argv);
MongoTripodConfig::setConfig(json_decode(file_get_contents($argv[0]),true));

define('MONGO_MAIN_DB',$argv[1]);
define('MONGO_MAIN_COLLECTION',$argv[2]);

$esSearchDocSpecs = MongoTripodConfig::getInstance()->esSearchDocSpecs;
$esqConfig = MongoTripodConfig::getInstance()->queue;

$types = (empty($argv[3])) ? null : array($argv[3]);
$_id = (empty($argv[4])) ? null : $argv[4];

if ($types == null)
{
    $types = array();
    // get types from mongo config
    foreach ($esSearchDocSpecs as $spec)
    {
        $types[] = $spec['type'];
        print "found index view spec for type {$spec['type']}\n";
    }
}

$m = new Mongo($esqConfig['connStr']);
$qdb = $m->selectDB($esqConfig['database']);

// find resources to queue
$tripod = new MapReduce();
if (empty($_id))
{
    $cursor = $tripod->selectCursor(array("rdf:type.value"=>array('$in'=>$types)),array('_id'=>1,'rdf:type'=>1));
}
else
{
    $cursor = $tripod->selectCursor(array("_id"=>$_id,"rdf:type.value"=>array('$in'=>$types)),array('_id'=>1,'rdf:type'=>1));
}

foreach($cursor as $resource)
{
    $tripod->getQueue()->addItem($resource["_id"], array($resource['rdf:type']['value']), MONGO_MAIN_DB, MONGO_MAIN_COLLECTION);
}
