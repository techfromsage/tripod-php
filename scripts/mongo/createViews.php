<?php

$options = getopt(
    "c:s:q:hv:i:a",
    array(
        "config:",
        "storename:",
        "tripod-dir:",
        "arc-dir:",
        "spec:",
        "id:",
        "help",
        "stat-loader:",
        "queue",
        "async"
    )
);

function showUsage()
{
    $help = <<<END
createViews.php

Usage:

php createViews.php -c/--config path/to/tripod-config.json -s/--storename store-name [options]

Options:
    -h --help               This help
    -c --config             path to Config configuration (required)
    -s --storename          Store to create views for (required)
    -v --spec               Only create for specified view specs
    -i --id                 Resource ID to regenerate views for
    -a --async              Generate table rows via queue
    -q --queue              Queue name to place jobs on (defaults to configured TRIPOD_APPLY_QUEUE value)

    --stat-loader           Path to script to initialize a Stat object.  Note, it *must* return an iTripodStat object!
    --tripod-dir            Path to tripod directory base
    --arc-dir               Path to ARC2 (required with --tripod-dir)

END;
    echo $help;
}

if(empty($options) || isset($options['h']) || isset($options['help']) ||
    (!isset($options['c']) && !isset($options['config'])) ||
    (!isset($options['s']) && !isset($options['storename']))
)
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
require_once 'classes/Timer.class.php';
require_once 'mongo/TripodConfigs.php';
require_once 'mongo/Tripods.php';

/**
 * @param string|null $id
 * @param string|null $viewId
 * @param string $storeName
 * @param \Tripod\ITripodStat|null $stat
 */
function generateViews($id, $viewId, $storeName, $stat, $queue)
{
    $viewSpec = \Tripod\Mongo\Config::getInstance()->getViewSpecification($storeName, $viewId);
    if(empty($viewSpec)) // Older version of Tripod being used?
    {
        $viewSpec = \Tripod\Mongo\Config::getInstance()->getViewSpecification($viewId);
    }
    echo $viewId;
    if (array_key_exists("from",$viewSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $viewId";
        $tripod = new \Tripod\Mongo\Driver($viewSpec['from'], $storeName, array('stat'=>$stat));
        $views = $tripod->getTripodViews();//new Views($tripod->storeName,$tripod->collection,$tripod->defaultContext);
        if ($id)
        {
            print " for $id....\n";
            $views->generateView($viewId, $id, null, $queue);
        }
        else
        {
            print " for all views....\n";
            $views->generateView($viewId, null, null, $queue);
        }
    }
}

$t = new \Tripod\Timer();
$t->start();

\Tripod\Mongo\Config::setConfig(json_decode(file_get_contents($configLocation),true));

if(isset($options['s']) || isset($options['storename']))
{
    $storeName = isset($options['s']) ? $options['s'] : $options['storename'];
}
else
{
    $storeName = null;
}

if(isset($options['v']) || isset($options['spec']))
{
    $viewId = isset($options['v']) ? $options['v'] : $options['spec'];
}
else
{
    $viewId = null;
}

if(isset($options['i']) || isset($options['id']))
{
    $id = isset($options['i']) ? $options['i'] : $options['id'];
}
else
{
    $id = null;
}

$queue = null;
if(isset($options['a']) || isset($options['async']))
{
    if(isset($options['q']) || isset($options['queue']))
    {
        $queue = $options['queue'];
    }
    else
    {
        $queue = MongoTripodConfig::getInstance()->getApplyQueueName();
    }
}

$stat = null;

if(isset($options['stat-loader']))
{
    $stat = include_once $options['stat-loader'];
}

if ($viewId)
{
    generateViews($id, $viewId, $storeName, $stat, $queue);
}
else
{
    foreach(\Tripod\Mongo\Config::getInstance()->getViewSpecifications($storeName) as $viewSpec)
    {
        generateViews($id, $viewSpec['_id'], $storeName, $stat, $queue);
    }
}

$t->stop();
print "Views created in ".$t->result()." secs\n";