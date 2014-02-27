<?php

set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'tripod.inc.php';
require_once 'mongo/util/TriplesUtil.class.php';

if ($argc!=2)
{
    echo "usage: ./BSONToQuads.php tripodConfig.json < bsondata\n";
    echo "  When exporting bson data from Mongo use:  \n";
    echo "     mongoexport -d <dbname> -c <collectionName> > bsondata.txt \n";
    die();
}

array_shift($argv);
$config = json_decode(file_get_contents($argv[0]), true);
MongoTripodConfig::setConfig($config);

$tu = new TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);
    $doc = json_decode($line, true);
    $context = $doc['_id']['c'];

    $graph = new MongoGraph();
    $graph->add_tripod_array($doc);

    echo $graph->to_nquads($context);
}
?>