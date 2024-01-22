<?php

require_once __DIR__ . '/../vendor/autoload.php';

// Queue worker must register these event listeners
Resque_Event::listen('beforePerform', [\Tripod\Mongo\Jobs\JobBase::class, 'beforePerform']);
Resque_Event::listen('onFailure', [\Tripod\Mongo\Jobs\JobBase::class, 'onFailure']);
