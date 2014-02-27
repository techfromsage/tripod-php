<?php
set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src'
  . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src/classes');

require_once 'tripod.inc.php';
require_once 'mongo/MongoTripodConfig.class.php';
require_once 'mongo/MongoTripod.class.php';

if (count($argv)!=2)
{
	echo "usage: ./setup.php tripodConfig.json\n";
	die();
}

MongoTripodConfig::setConfig(json_decode(file_get_contents($argv[1]),true));

//TODO Pull this into a utility class and also use in ElasticSearchTest
$config = MongoTripodConfig::getInstance();
foreach($config->esIndexes as $indexName=>$indexConfig)
{
    exec("curl -XDELETE {$config->esEndpoint}/$indexName");
    $jsonConfig = json_encode($indexConfig);
    exec("curl -XPOST {$config->esEndpoint}/$indexName -d '{$jsonConfig}'");
}
