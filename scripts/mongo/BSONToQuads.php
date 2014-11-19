<?php

set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'tripod.inc.php';
require_once 'mongo/util/TriplesUtil.class.php';

if ($argc<2)
{
    echo "usage: ./BSONToQuads.php tripodConfig.json [configSpec] < bsondata\n";
    echo "  When exporting bson data from Mongo use:  \n";
    echo "     mongoexport -d <dbname> -c <collectionName> > bsondata.txt \n";
    die();
}

array_shift($argv);
$config = json_decode(file_get_contents($argv[0]), true);
$configSpec = (isset($argv[1]) ? $argv[1] : MongoTripodConfig::DEFAULT_CONFIG_SPEC);

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

$tu = new TriplesUtil($configSpec);

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);
    $doc = json_decode($line, true);
    $context = $doc['_id']['c'];

    $graph = new MongoGraph($configSpec);
    $graph->add_tripod_array($doc);

    echo $graph->to_nquads($context);
}
?>