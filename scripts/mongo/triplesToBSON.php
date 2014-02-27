<?php

set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'tripod.inc.php';
require_once 'mongo/util/TriplesUtil.class.php';

if ($argc!=2)
{
	echo "usage: ./triplesToBSON.php tripodconfig.json < ntriplesdata\n";
	die();
}
array_shift($argv);

$config = json_decode(file_get_contents($argv[0]), true);
MongoTripodConfig::setConfig($config);

$currentSubject = "";
$triples = array();
$docs = array();
$errors = array(); // array of subjects that failed to insert, even after retry...
$tu = new TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $line = rtrim($line);

    $parts = preg_split("/\s/",$line);
    $subject = trim($parts[0],'><');

    if (empty($currentSubject)) // set for first iteration
    {
        $currentSubject = $subject;
    }

    if ($currentSubject!=$subject) // once subject changes, we have all triples for that subject, flush to Mongo
    {
        print(json_encode($tu->getTArrayAbout($currentSubject,$triples,MongoTripodConfig::getInstance()->getDefaultContextAlias()))."\n");
        $currentSubject=$subject; // reset current subject to next subject
        $triples = array(); // reset triples
    }

    $triples[] = $line;
}

// last doc
print(json_encode($tu->getTArrayAbout($currentSubject,$triples,MongoTripodConfig::getInstance()->getDefaultContextAlias()))."\n");

?>

