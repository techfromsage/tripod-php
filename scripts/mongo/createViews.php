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

function generateViews($id, $viewId,$configSpec)
{
    $viewSpec = MongoTripodConfig::getInstance($configSpec)->getViewSpecification($viewId);
    echo $viewId;
    if (array_key_exists("from",$viewSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $viewId";
        $tripod = new MongoTripod($viewSpec['from'], array('configSpec'=>$configSpec));
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

$options = getopt('c:s:v:i:');
if (!isset($options['c']))
{
	echo "usage: ./createViews.php -c tripodConfig.json [-s configSpec] [-v viewId] [-i _id]\n";
	die();
}

$configSpec = (isset($options['s']) ? $options['s'] : MongoTripodConfig::DEFAULT_CONFIG_SPEC);
$config = json_decode(file_get_contents($options['c']),true);

// Rewrite single config model as configSpec
$configKeys = array('namespaces', 'databases', 'defaultContext');
if(count(array_intersect($configKeys, array_keys($config))) > 1)
{
    $config = array($configSpec=>$config);
}

if(isset($config[$configSpec]))
{
    foreach($config as $spec=>$cfg)
    {
        MongoTripodConfig::setConfig($cfg, $spec);
    }
}
else
{
    throw new MongoTripodConfigException("ConfigSpec not defined in configuration document");
}

$viewId = (isset($options['v']) ? $options['v'] : null);
$id = (isset($options['i']) ? $options['i'] : null);
if ($viewId)
{
    generateViews($id, $viewId, $configSpec);
}
else
{
    foreach(MongoTripodConfig::getInstance($configSpec)->getViewSpecifications() as $viewSpec)
    {
        generateViews($id, $viewSpec['_id'], $configSpec);
    }
}

$t->stop();
print "Views created in ".$t->result()." secs\n";