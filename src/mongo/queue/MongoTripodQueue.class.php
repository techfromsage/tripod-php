<?php

require_once TRIPOD_DIR . 'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR . 'mongo/ModifiedSubject.class.php';
require_once TRIPOD_DIR . 'exceptions/TripodException.class.php';
require_once TRIPOD_DIR . 'mongo/delegates/MongoTripodTables.class.php';

// TODO: need to put an index on createdDate, lastUpdatedDate and status
class MongoTripodQueue extends MongoTripodBase
{
    protected $queueConfig;
    public function __construct($configSpec = MongoTripodConfig::DEFAULT_CONFIG_SPEC, $stat=null)
    {
        $this->configSpec = $configSpec;
        $this->config = $this->getMongoTripodConfigInstance();
        $this->queueConfig = $this->config->getQueueConfig();
        $connStr = $this->config->getQueueConnStr();

        $this->debugLog("Connecting to queue with $connStr");
        if(isset($this->queueConfig['replicaSet']) && !empty($this->queueConfig['replicaSet'])) {
            $this->debugLog("Connecting to replica set {$this->queueConfig['replicaSet']}");
            $m = new MongoClient($connStr, array("replicaSet"=>$this->queueConfig['replicaSet']));
        } else {
            $m = new MongoClient($connStr);
        }

        // select a database
        $this->db = $m->selectDB($this->queueConfig['database']);
        $this->collectionName = $this->queueConfig['collection'];
        $this->collection = $this->db->selectCollection($this->collectionName);

        if ($stat!=null) $this->stat = $stat;
    }

    /**
     * Processes the next item on the queue
     * @return bool - false if there is no next item to process, otherwise true
     */
    public function processNext()
    {
        $now = new MongoDate();
        $queuedItem = $this->fetchNextQueuedItem();
        if($queuedItem !== NULL)
        {
            $data = $queuedItem->getData();
            /* @var $createdOn MongoDate */
            $createdOn = $data['createdOn'];

            $tripod = $this->getMongoTripod($data);

            $observers = array();
            $operations = $data['operations'];
            foreach ($operations as $operation)
            {
                $observers[] = $tripod->getObserver($operation);
            }

            foreach($observers as $observer)
            {
                /* @var $observer SplObserver */
                $queuedItem->attach($observer);
            }

            try
            {
                // notify observers
                $this->debugLog("Queue processing item {$data['r']} with operations ".implode(", ",$operations));
                $queuedItem->notify();
                $this->removeItem($queuedItem);
            }
            catch(Exception $e)
            {
                $this->errorLog("Error processing item in queue: ".$e->getMessage(),array("data"=>$data));
                $this->failItem($queuedItem, $e->getMessage()."\n".$e->getTraceAsString());
            }
            // stat time taken to process item, from time it was created (queued)
            $timeTaken = ($now->usec/1000 + $now->sec*1000) - ($createdOn->usec/1000 + $createdOn->sec*1000);
            $this->getStat()->timer(MONGO_QUEUE_SUCCESS,$timeTaken);
            return true;
        }
        return false;
    }

    /**
     * @param array $data
     * @return MongoTripod
     */
    protected function getMongoTripod(Array $data) {
        $opts = array('stat'=>$this->stat);
        if(isset($data['configSpec']))
        {
            $opts['configSpec'] = $data['configSpec'];
        }
        elseif(isset($data['database'])) // Backwards compatibility
        {
            $opts['configSpec'] = MongoTripodConfig::getSpecNameForDatabaseAndCollection($data['database'], $data['collection']);
        }
        return new MongoTripod($data['collection'], $opts);
    }

    /**
     * Add an item to the index queue
     * @param ModifiedSubject $subject
     */
    public function addItem(ModifiedSubject $subject)
    {
        $data = $subject->getData();
        $data["_id"] = $this->getUniqId();
        $data["createdOn"] = new MongoDate();
        $data['status'] = "queued";
        $this->collection->insert($data);
    }

    /**
     * Removes an item from the index queue
     *
     * @param $subject ModifiedSubject the item to remove from the queue
     */
    public function removeItem(ModifiedSubject $subject)
    {
        $data = $subject->getData();
        $id = $data['_id'];
        $this->collection->remove(array("_id"=>$id));
    }

    /**
     * This method updates the status of the queued item to failed; and logs any error message you specify
     *
     * @param $subject ModifiedSubject the item to fail
     * @param $errorMessage, any error message you wish to be logged with the queued item
     */
    public function failItem(ModifiedSubject $subject, $errorMessage=null)
    {
        $data = $subject->getData();
        $id = $data['_id'];
        $this->collection->update(
            array("_id"=>$id),
            array('$set'=>array('status'=>'failed', 'lastUpdated'=>new MongoDate(), 'errorMessage'=>$errorMessage)),
            array('upsert'=>false, 'multiple'=>false)
        );
        $this->getStat()->increment(MONGO_QUEUE_FAIL);
    }

    /**
     * This item grabs the next queued item, it sets the state of the queued item to processing before returning it.
     *
     * @return null|ModifiedSubject if nothing in the queue, otherwise it returns the first document it finds.
     * @throws Exception
     */
    public function fetchNextQueuedItem()
    {
        $response = $this->db->command(array(
            "findAndModify" => $this->collectionName,
            "query" => array("status"=>"queued"),
            "update" => array('$set'=>array(
                "status"=>"processing",
                'lastUpdated'=>new MongoDate())
                ),
            'new'=>true
         ));
        if ($response["ok"]==true)
        {
            if(!empty($response['value']))
            {
                return new ModifiedSubject($response['value']);
            }
            else
            {
                // nothing in the queue
                return NULL;
            }
        }
        else
        {
            throw new Exception("Fetch Next Queued Item, Find and update failed!\n" . print_r($response, true));
        }
    }

    /**
     * This method retrieves an item from the queue based on the unique qid
     * only use this if you know the id of the queued item; in all other cases
     * you should use fetchNextQueuedItem()
     *
     * @param $_id, the id of the item in the queue
     * @return array|null
     */
    public function getItem($_id)
    {
        return $this->collection->findOne(array('_id'=>$_id));
    }

    /**
     * This method returns the number of items currently on the queue
     * @return int
     */
    public function count()
    {
        return $this->collection->count();
    }

    /**
     * Deletes everything from the queue
     */
    public function purgeQueue()
    {
        $this->collection->drop();
    }

    protected function getUniqId()
    {
        return new MongoId();
    }
}
