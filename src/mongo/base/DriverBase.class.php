<?php

namespace Tripod\Mongo;

require_once TRIPOD_DIR.'ITripodStat.php';

$TOTAL_TIME=0;

use Monolog\Logger;
use Tripod\Exceptions\Exception;
use Tripod\IEventHook;
use \MongoDB\Collection;

/**
 * Class DriverBase
 * @package Tripod\Mongo
 */
abstract class DriverBase
{
    /**
     * constants for the supported hook functions that can be applied
     */
    const HOOK_FN_PRE = "pre";
    const HOOK_FN_SUCCESS = "success";
    const HOOK_FN_FAILURE = "failure";

    /**
     * @var Collection
     */
    protected $collection;

    /**
     * @var string
     */
    protected $storeName;

    /**
     * @var string
     */
    protected $podName;

    /**
     * @var $podName
     */
    protected $defaultContext;

    /**
     * @var \Tripod\iTripodStat
     */
    protected $stat = null;

    /** @var array  */
    protected $statsConfig = array();

    /**
     * @var \MongoDB
     */
    protected $db = null;

    /**
     * @var string
     */
    protected $readPreference;

    /**
     * @return \Tripod\ITripodStat
     */
    public function getStat()
    {
        if ($this->stat==null)
        {
            $this->setStat($this->getStatFromStatFactory($this->statsConfig));
        }
        return $this->stat;
    }

    /**
     * For mocking out the creation of stat objects
     * @param array $config
     * @return \Tripod\ITripodStat
     */
    protected function getStatFromStatFactory(array $config)
    {
        return \Tripod\TripodStatFactory::create($this->statsConfig);
    }

    /**
     * @param \Tripod\ITripodStat $stat
     */
    public function setStat(\Tripod\ITripodStat $stat)
    {
        // TODO: how do we decouple this and still allow StatsD to know which db we're using?
        if($stat instanceof \Tripod\StatsD)
        {
            $stat->setPivotValue($this->getStoreName());
        }
        $this->stat = $stat;
    }

    /**
     * Returns stat object config
     * @return array
     */
    public function getStatsConfig()
    {
        $stat = $this->getStat();
        if($stat)
        {
            $statConfig = $stat->getConfig();
        }
        else
        {
            $statConfig = $this->statsConfig;
        }
        return $statConfig;
    }
    /**
     * @var Labeller
     */
    protected $labeller;

    /**
     * @var Config
     */
    protected $config = null;

    /**
     * @param int $secs
     * @return int
     */
    protected function getExpirySecFromNow($secs)
    {
        return (time()+$secs);
    }

    /**
     * @param string|null $context
     * @return mixed
     */
    protected function getContextAlias($context=null)
    {
        $contextAlias = $this->labeller->uri_to_alias((empty($context)) ? $this->defaultContext : $context);
        return (empty($contextAlias)) ? Config::getInstance()->getDefaultContextAlias() : $contextAlias;
    }

    /**
     * @param array $query
     * @param string $type
     * @param Collection|null $collection
     * @param array $includeProperties
     * @param int $cursorSize
     * @return MongoGraph
     */
    protected function fetchGraph($query, $type, $collection=null,$includeProperties=array(), $cursorSize=101)
    {
        $graph = new MongoGraph();

        $t = new \Tripod\Timer();
        $t->start();

        if ($collection==null)
        {
            $collection = $this->collection;
            $collectionName = $collection->getCollectionName();
        }
        else
        {
            $collectionName = $collection->getCollectionName();
        }

        if (empty($includeProperties))
        {
            $cursor = $collection->find($query);
        }
        else
        {
            $fields = array();
            foreach ($includeProperties as $property)
            {
                $fields[$this->labeller->uri_to_alias($property)] = true;
            }
            $cursor = $collection->find($query, array(
                'projection' => $fields,
                'batchSize' => $cursorSize
            ));
        }

        $ttlExpiredResources = false;

        $retries = 1;
        $exception = null;
        $cursorSuccess = false;

        do {
            try {
                foreach($cursor as $result)
                {
                    // handle MONGO_VIEWS that have expired due to ttl. These are expired
                    // on read (lazily) rather than on write
                    if ($type==MONGO_VIEW && array_key_exists(_EXPIRES,$result['value']))
                    {
                        // if expires < current date, regenerate view..
                        $currentDate = new \MongoDate();
                        if ($result['value'][_EXPIRES]<$currentDate)
                        {
                            // regenerate!
                            $this->generateView($result['_id']['type'],$result['_id']['r']);
                        }
                    }
                    $graph->add_tripod_array($result);
                }
                $cursorSuccess = true;
            } catch (\MongoCursorException $e) {
                self::getLogger()->error("MongoCursorException attempt ".$retries.". Retrying...:" . $e->getMessage());
                sleep(1);
                $retries++;
                $exception = $e;
            }
        } while ($retries <= Config::CONNECTION_RETRIES && $cursorSuccess === false);

        if ($cursorSuccess === false) {
            self::getLogger()->error("MongoCursorException failed after " . $retries . " attempts (MAX:".Config::CONNECTION_RETRIES."): " . $e->getMessage());
            throw new \MongoCursorException($exception);
        }

        if ($ttlExpiredResources)
        {
            // generate views and retry...
            $this->debugLog("One or more view had exceeded TTL was regenerated - request again...");
            $graph = $this->fetchGraph($query,$type,$collection);
        }

        $t->stop();
        $this->timingLog($type, array('duration'=>$t->result(), 'query'=>$query, 'collection'=>$collectionName));
        if ($type==MONGO_VIEW)
        {
            if (array_key_exists("_id.type",$query))
            {
                $this->getStat()->timer("$type.{$query["_id.type"]}",$t->result());
            }
            else if (array_key_exists("_id",$query) && array_key_exists("type",$query["_id"]))
            {
                $this->getStat()->timer("$type.{$query["_id"]["type"]}",$t->result());
            }
        }
        else
        {
            $this->getStat()->timer("$type.$collectionName",$t->result());
        }

        return $graph;
    }

    /**
     * @return string
     */
    public function getStoreName()
    {
        return $this->storeName;
    }

    /**
     * @return string
     */
    public function getPodName()
    {
        return $this->podName;
    }


    ///////// LOGGING METHODS BELOW ////////

    // @codeCoverageIgnoreStart

    /**
     * @param string $type
     * @param array|null $params
     */
    public function timingLog($type, $params=null)
    {
        $type = "[PID " . getmypid() ."] " . $type;
        $this->log(\Psr\Log\LogLevel::INFO,$type,$params); // todo: timing log is a bit weird. Should it infact go in a different channel? Is it just debug?
    }

    /**
     * @param string $message
     * @param array|null $params
     */
    /**
     * @param $message
     * @param array|null $params
     */
    public function infoLog($message, $params=null)
    {
        $message = "[PID " . getmypid() ."] " . $message;
        $this->log(\Psr\Log\LogLevel::INFO,$message,$params);
    }

    /**
     * @param $message
     * @param array|null $params
     */
    public function debugLog($message, $params=null)
    {
        $message = "[PID " . getmypid() ."] " . $message;
        $this->log(\Psr\Log\LogLevel::DEBUG,$message,$params);
    }

    /**
     * @param string $message
     * @param array|null $params
     */
    public function errorLog($message, $params=null)
    {
        $message = "[PID " . getmypid() ."] " . $message;
        $this->log(\Psr\Log\LogLevel::ERROR,$message,$params);
    }

    /**
     * @param string $message
     * @param array|null $params
     */
    public function warningLog($message, $params=null)
    {
        $message = "[PID " . getmypid() ."] " . $message;
        $this->log(\Psr\Log\LogLevel::WARNING,$message,$params);
    }

    /**
     * @param string $level
     * @param string $message
     * @param array|null $params
     */
    private function log($level, $message, $params)
    {
        ($params==null) ? self::getLogger()->log($level, $message) : self::getLogger()->log($level, $message, $params);
    }

    /**
     * @var \Psr\Log\LoggerInterface
     */
    public static $logger;

    /**
     * @static
     * @return \Psr\Log\LoggerInterface;
     */
    public static function getLogger()
    {
        if (self::$logger == null)
        {
            $log = new \Monolog\Logger('TRIPOD');
            self::$logger = $log;
        }
        return self::$logger;
    }
    // @codeCoverageIgnoreEnd

    /**
     * Expands an RDF sequence into proper tripod join clauses
     * @param array $joins
     * @param array $source
     */
    protected function expandSequence(&$joins, $source)
    {
        if(!empty($joins) && isset($joins['followSequence'])){
            // add any rdf:_x style properties in the source to the joins array,
            // up to rdf:_1000 (unless a max is specified in the spec)
            $max = (isset($joins['followSequence']['maxJoins'])) ? $joins['followSequence']['maxJoins'] : 1000;
            for($i=0; $i<$max; $i++) {
                $r = 'rdf:_' . ($i+1);

                if(isset($source[$r])){
                    $joins[$r] = array();
                    foreach($joins['followSequence'] as $k=>$v){
                        if($k != 'maxJoins') {
                            $joins[$r][$k] = $joins['followSequence'][$k];
                        } else {
                            continue;
                        }
                    }
                }
            }
            unset($joins['followSequence']);
        }
    }

    /**
     * Adds an _id object (or array of _id objects) to the target document's impact index
     *
     * @param array $id
     * @param array &$target
     * @throws \InvalidArgumentException
     */
    protected function addIdToImpactIndex(array $id, &$target, $buildImpactIndex=true)
    {
        if ($buildImpactIndex)
        {
            if(isset($id[_ID_RESOURCE]))
            {
                // Ensure that our id is curie'd
                $id[_ID_RESOURCE] = $this->labeller->uri_to_alias($id[_ID_RESOURCE]);
                if (!isset($target[_IMPACT_INDEX]))
                {
                    $target[_IMPACT_INDEX] = array();
                }
                if(!in_array($id, $target[_IMPACT_INDEX]))
                {
                    $target[_IMPACT_INDEX][] = $id;
                }
            }
            else // Assume this is an array of ids
            {
                foreach($id as $i)
                {
                    if(!isset($i[_ID_RESOURCE]))
                    {
                        throw new \InvalidArgumentException("Invalid id format");
                    }
                    $this->addIdToImpactIndex($i, $target);
                }
            }
        }
    }

    /**
     * For mocking
     * @return Config
     */
    protected function getConfigInstance()
    {
        return Config::getInstance();
    }

    /**
     * @return\MongoDB
     */
    protected function getDatabase()
    {
        if(!isset($this->db))
        {
            $this->db = $this->config->getDatabase(
                $this->storeName,
                $this->config->getDataSourceForPod($this->storeName, $this->podName),
                $this->readPreference
            );
        }
        return $this->db;
    }

    /**
     * @return Collection
     */
    protected function getCollection()
    {
        if(!isset($this->collection))
        {
            $this->collection = $this->getDatabase()->selectCollection($this->podName);
        }

        return $this->collection;
    }

    protected function applyHooks($fn,$hooks,$args=array())
    {
        switch ($fn) {
            case $this::HOOK_FN_PRE:
            case $this::HOOK_FN_SUCCESS:
            case $this::HOOK_FN_FAILURE:
                break;
            default:
                throw new Exception("Invalid hook function $fn requested");
        }
        foreach ($hooks as $hook)
        {
            try
            {
                /* @var $hook IEventHook */
                $hook->$fn($args);
            }
            catch (\Exception $e)
            {
                // don't let rabid hooks stop tripod
                $this->getLogger()->error("Hook ".get_class($hook)." threw exception {$e->getMessage()}, continuing");
            }
        }
    }

}

/**
 * Class NoStat
 * @package Tripod\Mongo
 */
final class NoStat implements \Tripod\ITripodStat
{
    /**
     * @var self
     */
    public static $instance = null;

    /**
     * @param string $operation
     * @param int|number $inc
     * @return void
     */
    public function increment($operation, $inc = 1)
    {
        // do nothing
    }

    /**
     * @param string $operation
     * @param number $duration
     * @return void
     */
    public function timer($operation, $duration)
    {
        // do nothing
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return array();
    }

    /**
     * @return self
     */
    public static function getInstance()
    {
        if (self::$instance == null)
        {
            self::$instance = new NoStat();
        }
        return self::$instance;
    }

    /**
     * @param array $config
     * @return NoStat
     */
    public static function createFromConfig(array $config = array())
    {
        return self::getInstance();
    }
}

