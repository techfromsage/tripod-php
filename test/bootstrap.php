<?php

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/../src/tripod.inc.php';

/*
 * Mongo Config For Main DB
 */
define('MONGO_MAIN_DB', 'acorn');
define('MONGO_MAIN_COLLECTION', 'CBD_harvest');
define('MONGO_USER_COLLECTION', 'CBD_user');

// Queue worker must register these event listeners
Resque_Event::listen('beforePerform', [Tripod\Mongo\Jobs\JobBase::class, 'beforePerform']);
Resque_Event::listen('onFailure', [Tripod\Mongo\Jobs\JobBase::class, 'onFailure']);

// Make sure log statements don't go to stdout during tests...
$log = new Monolog\Logger('unittest');
$log->pushHandler(new Monolog\Handler\NullHandler());
Tripod\Mongo\DriverBase::$logger = $log;
