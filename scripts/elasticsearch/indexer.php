<?php
set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/MongoTripod.class.php';
require_once 'mongo/MongoTripodQueue.class.php';
require_once 'mongo/MongoTripodSearchIndexer.class.php';

if ($argc!=2)
{
	echo "usage: ./indexer.php tripodConfig.json n";
	die();
}

ini_set("memory_limit","320M");

array_shift($argv);
MongoTripodConfig::setConfig(json_decode(file_get_contents($argv[0]),true));

gc_enable();
echo "Memory limit: " . ini_get('memory_limit')."\n";
echo "Garbage Collection is enabled: " . var_export(gc_enabled(), true) . "\n";

echo("About to start indexing....\n");
$i=0;
$startTime = microtime(true);

$indexQueue = new MongoTripodQueue();
$tripod = null;
$searchIndexer = null;
$collectionName = null;
$databaseName = null;

while(true)
{
    if ((++$i)%1000 == 0)
    {
        echo '.';
    }

    if ($i%10000 == 0)
    {
        echo "\nGarbage Collecting: " . gc_collect_cycles() ."\n";
    }

    $queuedItem = $indexQueue->fetchNextQueuedItem();
    if($queuedItem !== NULL)
    {
        $_id = $queuedItem['_id'];

        if($collectionName != $queuedItem['collection'] || $databaseName != $queuedItem['database'])
        {
            $collectionName = $queuedItem['collection'];
            $databaseName = $queuedItem['database'];
            $tripod = new MapReduce($collectionName, $databaseName);
            $searchIndexer = new ElasticSearchIndexer($tripod);
        }

        try
        {
            // index but dont bother deleting first
            $searchIndexer->process($queuedItem, false);
            $indexQueue->removeItem($_id);
        }
        catch(Exception $e)
        {
            $indexQueue->failItem($_id, $e->getMessage()."\n".$e->getTraceAsString());
        }
    }
    else
    {
        if ($startTime !== null)
        {
            $duration = round(microtime(true) - $startTime, 0);
            $sec = $duration % 60;
            $min = ($duration - $sec) / 60;
            echo "\nDuration: {$duration} seconds\n";
            echo "\nDuration: {$min}m {$sec}s\n";

            $peak_memory_usage_bytes = memory_get_peak_usage(true);
            echo 'Peak Memory Usage (bytes): [' . $peak_memory_usage_bytes . ']' . "\n";

            $startTime = null; // Don't keep logging out duration is the indexer is looping

        }
        echo "\nNothing to index - waiting...\n";
        sleep(10);
    }
}
?>