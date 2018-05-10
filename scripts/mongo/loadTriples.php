<?php
require_once dirname(__FILE__) . '/common.inc.php';

require_once dirname(dirname(dirname(__FILE__))).'/src/tripod.inc.php';

/**
 * @param \Tripod\Mongo\TriplesUtil $loader
 * @param string $subject
 * @param array $triples
 * @param array $errors
 * @param string $podName
 * @param string $storeName
 */
function load(\Tripod\Mongo\TriplesUtil $loader,$subject,Array $triples,Array &$errors,$podName,$storeName)
{
    try
    {
        $loader->loadTriplesAbout($subject,$triples,$storeName,$podName);
    }
    catch (Exception $e)
    {
        print "Exception for subject $subject failed with message: ".$e->getMessage()."\n";
        $errors[] = $subject;
    }
}

$timer = new \Tripod\Timer();
$timer->start();

if ($argc!=4)
{
	echo "usage: ./loadTriples.php storename podname tripodConfig.json < ntriplesdata\n";
	die();
}
array_shift($argv);

$storeName = $argv[0];
$podName = $argv[1];
\Tripod\Config::setConfig(json_decode(file_get_contents($argv[2]),true));

$i=0;
$currentSubject = "";
$triples = array();
$errors = array(); // array of subjects that failed to insert, even after retry...
$loader = new \Tripod\Mongo\TriplesUtil();

while (($line = fgets(STDIN)) !== false) {
    $i++;

    if (($i % 250000)==0)
    {
        print "Memory: ".memory_get_usage()."\n";
    }

    $line = rtrim($line);
    $parts = preg_split("/\s/",$line);
    $subject = trim($parts[0],'><');


    if (empty($currentSubject)) // set for first iteration
    {
        $currentSubject = $subject;
    }
    else if ($currentSubject!=$subject) // once subject changes, we have all triples for that subject, flush to Mongo
    {
        load($loader,$currentSubject,$triples,$errors,$podName,$storeName);
        $currentSubject=$subject; // reset current subject to next subject
        $triples = array(); // reset triples
    }
    $triples[] = $line;
}

// last doc
load($loader,$currentSubject,$triples,$errors,$podName,$storeName);

$timer->stop();
print "This script ran in ".$timer->result()." milliseconds\n";

echo "Processed ".($i)." triples";
if (count($errors)>0)
{
    echo "Insert errors on ".count($errors)." subjects\n";
    var_dump($errors); //todo: decide what to do with errors...
}
