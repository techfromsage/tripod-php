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

function generateViews($id, $viewId,$dbName)
{
    $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewId);
    echo $viewId;
    if (array_key_exists("from",$viewSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $viewId";
        $tripod = new MongoTripod($viewSpec['from'], $dbName);
        $views = $tripod->getTripodViews();//new MongoTripodViews($tripod->db,$tripod->collection,$tripod->defaultContext);
        if ($id)
        {
            print " for $id....\n";
            $views->generateView($viewId, $id);
        }
        else
        {
            print " for all views....\n";
            $views->generateView($viewId, null);
        }
    }
}

$t = new Timer();
$t->start();

if ($argc!=3 && $argc!=4 && $argc!=5)
{
	echo "usage: ./createViews.php tripodConfig.json dbName [viewId] [_id]\n";
	die();
}
array_shift($argv);

MongoTripodConfig::setConfig(json_decode(file_get_contents($argv[0]),true));
$viewId = (empty($argv[2])) ? null: $argv[2];
$id = (empty($argv[3])) ? null: $argv[3];
if ($viewId)
{
    generateViews($id, $viewId, $argv[1]);
}
else
{
    foreach(MongoTripodConfig::getInstance()->getViewSpecifications() as $viewSpec)
    {
        generateViews($id, $viewSpec['_id'], $argv[1]);
    }
}

$t->stop();
print "Views created in ".$t->result()." secs\n";