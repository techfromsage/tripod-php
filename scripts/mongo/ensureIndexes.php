<?php
require_once dirname(__FILE__) . '/common.inc.php';

require_once dirname(dirname(dirname(__FILE__))).'/src/tripod.inc.php';

if ($argc!=2&&$argc!=3&&$argc!=4)
{
	echo "usage: php ensureIndexes.php tripodConfig.json [storeName] [forceReindex (default is false)] [background (default is true)]\n";
	die();
}
array_shift($argv);

\Tripod\Mongo\Config::setConfig(json_decode(file_get_contents($argv[0]),true));

$storeName = (isset($argv[1])) ? $argv[1] : null;
$forceReindex = (isset($argv[2])&&($argv[2]=="true")) ? true : false;
$background = (isset($argv[3])&&($argv[3]=="false")) ? false : true;

\Tripod\Mongo\Config::getInstance()->setMongoCursorTimeout(-1);

$ei = new \Tripod\Mongo\Jobs\EnsureIndexes();

$t = new \Tripod\Timer();
$t->start();
print("About to start scheduling indexing jobs for $storeName...\n");
$ei->createJob($storeName, $forceReindex, $background);
$t->stop();
print "Finished scheduling ensure indexes jobs, took {$t->result()} seconds\n";
