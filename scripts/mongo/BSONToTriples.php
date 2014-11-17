<?php

set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'tripod.inc.php';
require_once 'mongo/util/TriplesUtil.class.php';

if ($argc<2)
{
	echo "usage: ./BSONToTriples.php tripodConfig.json [configSpec] < bsondata\n";
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

    $graph = new MongoGraph($configSpec);
    $doc = json_decode($line, true);

    if(array_key_exists("_id", $doc)) {

        $subject = $doc['_id'];

        unset($doc["_id"]);
        if( array_key_exists("_version", $doc)) {
            unset($doc["_version"]);
        }

        foreach($doc as $property=>$values) {
            if(isset($values['value'])) {
                $doc[$property] = array($values);
            }
        }

        foreach($doc as $property=>$values) {
            foreach($values as $value) {
                if($value['type'] == "literal" ) {
                    $graph->add_literal_triple($subject, $graph->qname_to_uri($property), $value['value']);
                } else {
                    $graph->add_resource_triple($subject, $graph->qname_to_uri($property), $value['value']);
                }
            }
        }

        print($graph->to_ntriples());
    }
}
?>