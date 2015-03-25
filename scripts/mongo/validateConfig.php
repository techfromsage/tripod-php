<?php

$options = getopt(
    "c:h",
    array(
        "config:",
        "help"
    )
);

function showUsage()
{
    $help = <<<END
validateConfig.php

Usage:

php validateConfig.php -c/--config path/to/tripod-config.json [options]

Options:
    -h --help               This help
    -c --config             path to MongoTripodConfig configuration (required)
END;
    echo $help;
}

if(empty($options) || isset($options['h']) || isset($options['help']) || (!isset($options['c']) && !isset($options['config'])))
{
    showUsage();
    exit();
}
$configLocation = isset($options['c']) ? $options['c'] : $options['config'];

$tripodBasePath = dirname(dirname(dirname(__FILE__)));
set_include_path(
    get_include_path()
    . PATH_SEPARATOR . $tripodBasePath.'/src'
    . PATH_SEPARATOR . $tripodBasePath.'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripod.class.php';
require_once 'mongo/MongoTripodConfig.class.php';

MongoTripodConfig::setValidationLevel(MongoTripodConfig::VALIDATE_MAX);

MongoTripodConfig::setConfig(json_decode(file_get_contents($configLocation),true));

try {
    MongoTripodConfig::getInstance();

    echo "\nConfig OK\n";
}
catch(MongoTripodConfigException $e)
{
    echo "\nError: " . $e->getMessage() . "\n";
}
