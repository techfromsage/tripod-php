<?php
set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/'
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'classes/Timer.class.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/MongoTripod.class.php';

function generateTables($id, $tableId,$storeName)
{
    $tableSpec = MongoTripodConfig::getInstance()->getTableSpecification($storeName, $tableId);
    if (array_key_exists("from",$tableSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $tableId";
        $tripod = new MongoTripod($tableSpec['from'], $storeName);
        $tTables = $tripod->getTripodTables();//new MongoTripodTables($tripod->storeName,$tripod->collection,$tripod->defaultContext);
        if ($id)
        {
            print " for $id....\n";
            $tTables->generateTableRows($tableId, $id);
        }
        else
        {
            print " for all views....\n";
            $tTables->generateTableRows($tableId);
        }
    }
}

$t = new Timer();
$t->start();

if ($argc!=3 && $argc!=4 && $argc!=5)
{
    echo "usage: ./createTables.php tripodConfig.json storeName [tableId] [_id]\n";
    die();
}
array_shift($argv);

MongoTripodConfig::setConfig(json_decode(file_get_contents($argv[0]),true));
$tableId = (empty($argv[2])) ? null: $argv[2];
$id = (empty($argv[3])) ? null: $argv[3];
if ($tableId)
{
    generateTables($id, $tableId, $argv[1]);
}
else
{
    foreach(MongoTripodConfig::getInstance()->getTableSpecifications($argv[1]) as $tableSpec)
    {
        generateTables($id, $tableSpec['_id'], $argv[1]);
    }
}

$t->stop();
print "Tables created in ".$t->result()." secs\n";