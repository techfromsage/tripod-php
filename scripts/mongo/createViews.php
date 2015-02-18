<?php

$options = getopt(
    "c:s:hv:i:",
    array(
        "config:",
        "storename:",
        "tripod-dir:",
        "arc-dir:",
        "spec:",
        "id:",
        "help",
        "stat-loader:",
    )
);

function showUsage()
{
    $help = <<<END
createViews2.php

Usage:

php createViews2.php -c/--config path/to/tripod-config.json -s/--storename store-name [options]

Options:
    -h --help               This help
    -c --config             path to MongoTripodConfig configuration (required)
    -s --storename          Store to create views for (required)
    -v --spec               Only create for specified view specs
    -i --id                 Resource ID to regenerate views for

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
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/MongoTripod.class.php';

/**
 * @param string|null $id
 * @param string|null $viewId
 * @param string $storeName
 * @param iTripodStat|null $stat
 */
function generateViews($id, $viewId, $storeName, $stat)
{
    $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($storeName, $viewId);
    if(empty($viewSpec)) // Older version of Tripod being used?
    {
        $viewSpec = MongoTripodConfig::getInstance()->getViewSpecification($viewId);
    }
    echo $viewId;
    if (array_key_exists("from",$viewSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $viewId";
        $tripod = new MongoTripod($viewSpec['from'], $storeName, array('stat'=>$stat));
        $views = $tripod->getTripodViews();//new MongoTripodViews($tripod->storeName,$tripod->collection,$tripod->defaultContext);
        if ($id)
        {
            print " for $id....\n";
            $views->generateView($viewId, $id);
        }
        else
        {
            print " for all views....\n";
            $views->generateView($viewId, null);
        }
    }
}

$t = new Timer();
$t->start();

MongoTripodConfig::setConfig(json_decode(file_get_contents($configLocation),true));

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

$stat = null;

if(isset($options['stat-loader']))
{
    $stat = include_once $options['stat-loader'];
}

var_dump($stat);

die();

if ($viewId)
{
    generateViews($id, $viewId, $storeName, $stat);
}
else
{
    foreach(MongoTripodConfig::getInstance()->getViewSpecifications($storeName) as $viewSpec)
    {
        generateViews($id, $viewSpec['_id'], $storeName, $stat);
    }
}

$t->stop();
print "Views created in ".$t->result()." secs\n";