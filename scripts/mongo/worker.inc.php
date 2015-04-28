<?php

require "../../src/tripod.inc.php";
$logger = new \Psr\Log\NullLogger(); // silence resque // todo: implement PSR logger across Tripod.