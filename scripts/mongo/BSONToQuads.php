<?php

require_once dirname(__FILE__) . '/common.inc.php';
require_once dirname(dirname(dirname(__FILE__))).'/src/tripod.inc.php';

if ($argc!=2)
{
    echo "usage: ./BSONToQuads.php tripodConfig.json < bsondata\n";
    echo "  When exporting bson data from Mongo use:  \n";
    echo "     mongoexport -d <dbname> -c <collectionName> > bsondata.txt \n";
    die();
}

array_shift($argv);
$config = json_decode(file_get_contents($argv[0]), true);
\Tripod\Mongo\Config::setConfig($config);

$tu = new \Tripod\Mongo\TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);
    $doc = json_decode($line, true);
    $context = $doc['_id']['c'];

    $graph = new \Tripod\Mongo\MongoGraph();
    $graph->add_tripod_array($doc);

    echo $graph->to_nquads($context);
}
?>