<?php
set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripod.class.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/queue/MongoTripodQueue.class.php';

if ($argc < 2)
{
    echo "usage: ./processQueue.php tripodConfig.json [configSpec]";
    die();
}

ini_set("memory_limit","320M");

array_shift($argv);
$configSpec = (isset($argv[1]) ? $argv[1] : MongoTripodConfig::DEFAULT_CONFIG_SPEC);
$config = json_decode(file_get_contents($argv[0]),true);

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


gc_enable();
echo "Memory limit: " . ini_get('memory_limit')."\n";
echo "Garbage Collection is enabled: " . var_export(gc_enabled(), true) . "\n";

echo("About to start indexing....\n");
$i=0;
$startTime = microtime(true);

$queue = new MongoTripodQueue($configSpec);

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

    if (!$queue->processNext())
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
        echo "\nNothing to process - waiting...\n";
        sleep(10);
    }
}
?>