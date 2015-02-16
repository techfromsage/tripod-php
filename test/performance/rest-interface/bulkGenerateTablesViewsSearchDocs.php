<?php

require 'vendor/autoload.php';
define('ARC_DIR', dirname(__FILE__) . '/vendor/semsol/arc2/');
require_once dirname(__FILE__) . '/vendor/talis/tripod-php/src/tripod.inc.php';
define('TASK_TABLES', 'tables');
define('TASK_VIEWS', 'views');
define('TASK_SEARCH_DOCS', 'search');

$options = getopt("c:s:p:t:");

if(!isset($options['c']))
{
    $options['c'] = dirname(__FILE__) . '/config/config.json';
}

if(isset($options['t']))
{
    if(!is_array($options['t']))
    {
        $options['t'] = array($options['t']);
    }
    $validTasks = array(TASK_TABLES, TASK_VIEWS, TASK_SEARCH_DOCS);
    $tasks = array();
    foreach($options['t'] as $task)
    {
        if(!in_array($task, $validTasks))
        {
            throw new InvalidArgumentException("Option '-t' must be one of: " . implode(", ", $validTasks));
        }
        if(!in_array($task, $tasks))
        {
            $tasks[] = $task;
        }
    }
}
else
{
    $tasks = array(TASK_TABLES, TASK_VIEWS, TASK_SEARCH_DOCS);
}

$appConfig = json_decode(file_get_contents($options['c']), true);