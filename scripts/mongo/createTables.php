<?php

$options = getopt(
    "c:s:ht:i:",
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
createTables.php

Usage:

php createTables.php -c/--config path/to/tripod-config.json -s/--storename store-name [options]

Options:
    -h --help               This help
    -c --config             path to Config configuration (required)
    -s --storename          Store to create tables for (required)
    -t --spec               Only create for specified table specs
    -i --id                 Resource ID to regenerate table rows for

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
require_once 'mongo/Config.class.php';
require_once 'mongo/Tripod.class.php';

/**
 * @param string|null $id
 * @param string|null $tableId
 * @param string|null $storeName
 * @param iTripodStat|null $stat
 */
function generateTables($id, $tableId, $storeName, $stat = null)
{
    $tableSpec = TripodConfig::getInstance()->getTableSpecification($storeName, $tableId);
    if(empty($tableSpec)) // Older version of Tripod being used?
    {
        $tableSpec = TripodConfig::getInstance()->getTableSpecification($tableId);
    }
    if (array_key_exists("from",$tableSpec))
    {
        MongoCursor::$timeout = -1;

        print "Generating $tableId";
        $tripod = new MongoTripod($tableSpec['from'], $storeName, array('stat'=>$stat));
        $tTables = $tripod->getTripodTables();//new Tables($tripod->storeName,$tripod->collection,$tripod->defaultContext);
        if ($id)
        {
            print " for $id....\n";
            $tTables->generateTableRows($tableId, $id);
        }
        else
        {
            print " for all tables....\n";
            $tTables->generateTableRows($tableId);
        }
    }
}

$t = new Timer();
$t->start();

TripodConfig::setConfig(json_decode(file_get_contents($configLocation),true));

if(isset($options['s']) || isset($options['storename']))
{
    $storeName = isset($options['s']) ? $options['s'] : $options['storename'];
}
else
{
    $storeName = null;
}

if(isset($options['t']) || isset($options['spec']))
{
    $tableId = isset($options['t']) ? $options['t'] : $options['spec'];
}
else
{
    $tableId = null;
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

if ($tableId)
{
    generateTables($id, $tableId, $storeName, $stat);
}
else
{
    foreach(TripodConfig::getInstance()->getTableSpecifications($storeName) as $tableSpec)
    {
        generateTables($id, $tableSpec['_id'], $storeName, $stat);
    }
}

$t->stop();
print "Tables created in ".$t->result()." secs\n";