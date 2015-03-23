<?php

require_once TRIPOD_DIR.'ITripodStat.php';

$TOTAL_TIME=0;

abstract class MongoTripodBase
{
    /**
     * @var MongoCollection
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
     * @var iTripodStat
     */
    protected $stat = null;

    /**
     * @return ITripodStat
     */
    public function getStat()
    {
        return ($this->stat==null) ? NoStat::getInstance() : $this->stat;
    }

    /**
     * @var MongoTripodLabeller
     */
    protected $labeller;

    /**
     * @var MongoTripodConfig
     */
    protected $config = null;

    protected function getExpirySecFromNow($secs)
    {
        return (time()+$secs);
    }

    protected function getContextAlias($context=null)
    {
        $contextAlias = $this->labeller->uri_to_alias((empty($context)) ? $this->defaultContext : $context);
        return (empty($contextAlias)) ? MongoTripodConfig::getInstance()->getDefaultContextAlias() : $contextAlias;
    }

    /**
     * @param $query
     * @param $type
     * @param MongoCollection|null $collection
     * @param array $includeProperties
     * @param int $cursorSize
     * @return MongoGraph
     */
    protected function fetchGraph($query, $type, $collection=null,$includeProperties=array(), $cursorSize=101)
    {
        $graph = new MongoGraph();

        $t = new Timer();
        $t->start();

        if ($collection==null)
        {
            $collection = $this->collection;
            $collectionName = $collection->getName();
        }
        else
        {
            $collectionName = $collection->getName();
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
            $cursor = $collection->find($query,$fields);
        }

        $ttlExpiredResources = false;
        $cursor->batchSize($cursorSize);
        foreach($cursor as $result)
        {
            // handle MONGO_VIEWS that have expired due to ttl. These are expired
            // on read (lazily) rather than on write
            if ($type==MONGO_VIEW && array_key_exists(_EXPIRES,$result['value']))
            {
                // if expires < current date, regenerate view..
                $currentDate = new MongoDate();
                if ($result['value'][_EXPIRES]<$currentDate)
                {
                    // regenerate!
                    $this->generateView($result['_id']['type'],$result['_id']['r']);
                }
            }
            $graph->add_tripod_array($result);
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

    public function getStoreName()
    {
        return $this->storeName;
    }

    public function getPodName()
    {
        return $this->podName;
    }

    ///////// LOGGING METHODS BELOW ////////

    // @codeCoverageIgnoreStart

    // todo: tidy up logging mess and make it work consistently between projects
    public function timingLog($type, $params)
    {
        global $TOTAL_TIME;
        if (array_key_exists("duration",$params))
        {
            $TOTAL_TIME += $params["duration"];
            $params['cumulative'] = $TOTAL_TIME;
        }

        if (self::getLogger()!=null)
            self::getLogger()->getInstance()->debug('[TRIPOD_TIMING:'.$type.']', $params);
    }

    public function debugLog($message, $params=null)
    {
        if (self::getLogger()!=null)
        {
            ($params==null) ? self::getLogger()->getInstance()->debug("[TRIPOD_DEBUG] $message") : self::getLogger()->getInstance()->debug("[TRIPOD_DEBUG] $message", $params);
        }
        else
        {
            echo "$message\n";
            if ($params) print_r($params);
        }
    }

    public function errorLog($message, $params=null)
    {
        if (self::getLogger()!=null)
        {
            self::getLogger()->getInstance()->error("[TRIPOD_ERR] $message",$params);
        }
        else
        {
            echo "$message\n";
            if ($params)
            {
                echo "Params: \n";
                foreach ($params as $key=>$value)
                {
                    echo "$key: $value\n";
                }
            }
        }
    }

    public function warningLog($message, $params=null)
    {
        if (self::getLogger()!=null)
        {
            self::getLogger()->getInstance()->warn("[TRIPOD_WARN] $message",$params);
        }
        else
        {
            echo "$message\n";
            if ($params)
            {
                echo "Params: \n";
                foreach ($params as $key=>$value)
                {
                    echo "$key: $value\n";
                }
            }
        }
    }

    public static $logger;

    /**
     * @static
     * @return object a Logger
     */
    public static function getLogger()
    {
        if (self::$logger)
        {
            return self::$logger;
        }
        else
        {
            return null;
        }
    }
    // @codeCoverageIgnoreEnd

    /**
     * Expands an RDF sequence into proper tripod join clauses
     * @param $joins
     * @param $source
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
     * @throws InvalidArgumentException
     */
    protected function addIdToImpactIndex(array $id, &$target)
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
                    throw new InvalidArgumentException("Invalid id format");
                }
                $this->addIdToImpactIndex($i, $target);
            }
        }
    }

    /**
     * For mocking
     * @return MongoTripodConfig
     */
    protected function getMongoTripodConfigInstance()
    {
        return MongoTripodConfig::getInstance();
    }
}

final class NoStat implements ITripodStat
{
    public static $instance = null;
    public function increment($operation)
    {
        // do nothing
    }
    public function timer($operation, $duration)
    {
        // do nothing
    }

    public static function getInstance()
    {
        if (self::$instance == null)
        {
            self::$instance = new NoStat();
        }
        return self::$instance;
    }
}

