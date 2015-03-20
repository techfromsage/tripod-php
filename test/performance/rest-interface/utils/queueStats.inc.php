<?php

$queueOptions = json_decode(file_get_contents(dirname(__FILE__) . '/queue-stat-config.json'), true);

if(!(isset($queueOptions['host']) && isset($queueOptions['port']) && isset($queueOptions['env'])))
{
    throw new InvalidArgumentException("Must include 'host', 'port', and 'env'");
}
require_once dirname(dirname(__FILE__)) . '/src/Stat.class.php';
$qConfig = MongoTripodConfig::getInstance()->getQueueConfig();
StatConfig::setConfig(array('host'=>$queueOptions['host'],'port'=>$queueOptions['port']));
$stat = new Stat($qConfig['database']);
$stat->setStatEnvName($queueOptions['env']);
return $stat;
