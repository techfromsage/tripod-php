<?php
require_once dirname(__FILE__) . '/common.inc.php';
set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'classes/Timer.class.php';
require_once 'mongo/util/IndexUtils.class.php';
require_once 'mongo/Config.class.php';

if ($argc!=2&&$argc!=3&&$argc!=4)
{
	echo "usage: php ensureIndexes.php tripodConfig.json [storeName] [forceReindex (default is false)]\n";
	die();
}
array_shift($argv);

\Tripod\Mongo\Config::setConfig(json_decode(file_get_contents($argv[0]),true));

$storeName = (isset($argv[1])) ? $argv[1] : null;
$forceReindex = (isset($argv[2])&&($argv[2]=="true")) ? true : false;

\Tripod\Mongo\Config::getInstance()->setMongoCursorTimeout(-1);

$ei = new \Tripod\Mongo\IndexUtils();

$t = new \Tripod\Timer();
$t->start();
print("About to start indexing on $storeName...\n");
$ei->ensureIndexes($forceReindex,$storeName);
$t->stop();
print "Indexing complete, took {$t->result()} seconds\n";