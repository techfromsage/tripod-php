<?php

set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'classes/Timer.class.php';
require_once 'mongo/util/IndexUtils.class.php';
require_once 'mongo/MongoTripodConfig.class.php';

$options = getopt('c:s:d:f');
if (!isset($options['c']))
{
	echo "usage: php ensureIndexes.php -c tripodConfig.json [-s configSpec] [-d dbName] [-f {forceReindex (default is false)}]\n";
	die();
}
array_shift($argv);

$forceReindex = (isset($options['f']) ? true : false);

$configSpec = (isset($options['s']) ? $options['s'] : MongoTripodConfig::DEFAULT_CONFIG_SPEC);
$config = json_decode(file_get_contents($options['c']),true);
$dbName = (isset($options['d']) ? $options['d'] : null);

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

$ei = new IndexUtils();

$t = new Timer();
$t->start();
print("About to start indexing on $dbName...\n");
$ei->ensureIndexes($forceReindex, $configSpec, $db);
$t->stop();
print "Indexing complete, took {$t->result()} seconds\n";