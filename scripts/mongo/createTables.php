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

function generateTables($id, $tableId, $configSpec)
{
    $tableSpec = MongoTripodConfig::getInstance($configSpec)->getTableSpecification($tableId);
    if (array_key_exists("from",$tableSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $tableId";
        $tripod = new MongoTripod($tableSpec['from'], array('configSpec'=>$configSpec));
        $tTables = $tripod->getTripodTables();//new MongoTripodTables($tripod->db,$tripod->collection,$tripod->defaultContext);
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
$options = getopt('c:s:t:i:');
if (!isset($options['c']))
{
    echo "usage: ./createTables.php -c tripodConfig.json [-s configSpec] [-t tableId] [-i _id]\n";
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

$tableId = (isset($options['t']) ? $options['v'] : null);
$id = (isset($options['i']) ? $options['i'] : null);
if ($tableId)
{
    generateTables($id, $tableId, $configSpec);
}
else
{
    foreach(MongoTripodConfig::getInstance($configSpec)->getTableSpecifications() as $tableSpec)
    {
        generateTables($id, $tableSpec['_id'], $configSpec);
    }
}

$t->stop();
print "Tables created in ".$t->result()." secs\n";