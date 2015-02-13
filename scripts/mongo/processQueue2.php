<?php

$options = getopt(
  "c:h",
    array(
        "config:",
        "tripod-dir:",
        "help",
        "stat-loader:",
    )
);

function showUsage()
{
    $help = <<<END
processQueue2.php

Usage:

php processQueue2.php -c/--config path/to/tripod-config.json [options]

Options:
    -h --help               This help
    -c --config             path to MongoTripodConfig configuration (required)

    --stat-loader           Path to script to initialize a Stat object.  Note, it *must* return an iTripodStat object!
    --tripod-dir            Path to tripod directory base
    --arc-dir               Path to ARC2 (required with --tripod-dir)
END;
    echo $help;
}

if(empty($options) || isset($options['h']) || isset($options['help']) || (!isset($options['c']) && !isset($options['config'])))
{
    showUsage();
    exit();
}
$configLocation = isset($options['c']) ? $options['c'] : $options['config'];
if(isset($options['tripod-dir']))
{
    if(isset($options['arc-dir']))
    {
        $tripodBasePath = $options['tripod-dir'];
        define('ARC_DIR', $options['arc-dir']);
    }
    else
    {
        showUsage();
        exit();
    }
}
else
{
    $tripodBasePath = dirname(dirname(dirname(__FILE__)));
}
set_include_path(
    get_include_path()
    . PATH_SEPARATOR . $tripodBasePath.'/src'
    . PATH_SEPARATOR . $tripodBasePath.'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripod.class.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/queue/MongoTripodQueue.class.php';

ini_set("memory_limit","320M");

MongoTripodConfig::setConfig(json_decode(file_get_contents($configLocation),true));

$stat = null;

if(isset($options['stat-loader']))
{
    $stat = include_once $options['stat-loader'];
}
gc_enable();
echo "Memory limit: " . ini_get('memory_limit')."\n";
echo "Garbage Collection is enabled: " . var_export(gc_enabled(), true) . "\n";

echo("About to start indexing....\n");
$i=0;
$startTime = microtime(true);

$queue = new MongoTripodQueue($stat);

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