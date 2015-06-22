<?php
require_once dirname(__FILE__) . '/common.inc.php';
$options = getopt(
    "c:s:q:ht:i:a",
    array(
        "config:",
        "storename:",
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
createTables.php

Usage:

php createTables.php -c/--config path/to/tripod-config.json -s/--storename store-name [options]

Options:
    -h --help               This help
    -c --config             path to Config configuration (required)
    -s --storename          Store to create tables for (required)
    -t --spec               Only create for specified table specs
    -i --id                 Resource ID to regenerate table rows for
    -a --async              Generate table rows via queue
    -q --queue              Queue name to place jobs on (defaults to configured TRIPOD_APPLY_QUEUE value)

    --stat-loader           Path to script to initialize a Stat object.  Note, it *must* return an iTripodStat object!

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

require_once dirname(dirname(dirname(__FILE__))) . '/src/tripod.inc.php';

/**
 * @param string|null $id
 * @param string|null $tableId
 * @param string|null $storeName
 * @param \Tripod\ITripodStat|null $stat
 * @param string|null $queue
 */
function generateTables($id, $tableId, $storeName, $stat = null, $queue = null)
{
    $tableSpec = \Tripod\Mongo\Config::getInstance()->getTableSpecification($storeName, $tableId);
    if (array_key_exists("from",$tableSpec))
    {
        \Tripod\Mongo\Config::getInstance()->setMongoCursorTimeout(-1);

        print "Generating $tableId";
        $tripod = new \Tripod\Mongo\Driver($tableSpec['from'], $storeName, array('stat'=>$stat));
        $tTables = $tripod->getTripodTables();
        if ($id)
        {
            print " for $id....\n";
            $tTables->generateTableRows($tableId, $id);
        }
        else
        {
            print " for all tables....\n";
            $tTables->generateTableRows($tableId, null, null, $queue);
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

$queue = null;
if(isset($options['a']) || isset($options['async']))
{
    if(isset($options['q']) || isset($options['queue']))
    {
        $queue = $options['queue'];
    }
    else
    {
        $queue = \Tripod\Mongo\Config::getInstance()->getApplyQueueName();
    }
}

$stat = null;

if(isset($options['stat-loader']))
{
    $stat = include_once $options['stat-loader'];
}

if ($tableId)
{
    generateTables($id, $tableId, $storeName, $stat, $queue);
}
else
{
    foreach(\Tripod\Mongo\Config::getInstance()->getTableSpecifications($storeName) as $tableSpec)
    {
        generateTables($id, $tableSpec['_id'], $storeName, $stat, $queue);
    }
}

$t->stop();
print "Tables created in ".$t->result()." secs\n";