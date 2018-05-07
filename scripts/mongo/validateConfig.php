<?php
require_once dirname(__FILE__) . '/common.inc.php';
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
    -c --config             path to Config configuration (required)
END;
    echo $help;
}

if(empty($options) || isset($options['h']) || isset($options['help']) || (!isset($options['c']) && !isset($options['config'])))
{
    showUsage();
    exit();
}
$configLocation = isset($options['c']) ? $options['c'] : $options['config'];

require_once dirname(dirname(dirname(__FILE__))).'/src/tripod.inc.php';

\Tripod\Mongo\Config::setValidationLevel(\Tripod\Mongo\Config::VALIDATE_MAX);

\Tripod\Config::setConfig(json_decode(file_get_contents($configLocation),true));

try {
    \Tripod\Config::getInstance();

    echo "\nConfig OK\n";
}
catch(\Tripod\Exceptions\ConfigException $e)
{
    echo "\nError: " . $e->getMessage() . "\n";
}
