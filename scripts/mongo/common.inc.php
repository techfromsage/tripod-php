<?php
/**
 * This will load the composer autoloader depending on whether or not Tripod is the project (i.e. Tripod contains
 * a vendor subdirectory) or if Tripod is a dependency of another project (i.e. Tripod is *in* a vendor subdirectory)
 */
foreach (array(__DIR__ . '/../../../../autoload.php', __DIR__ . '/../../vendor/autoload.php') as $file) {
    if (file_exists($file)) {
        define('TRIPOD_COMPOSER_INSTALL', $file);
        break;
    }
}

if (!defined('TRIPOD_COMPOSER_INSTALL')) {
    die(
        'You need to set up the project dependencies using the following commands:' . PHP_EOL .
        'curl -sS https://getcomposer.org/installer | php -- --version 1.10.17' . PHP_EOL .
        'php composer.phar install' . PHP_EOL
    );
}

require TRIPOD_COMPOSER_INSTALL;