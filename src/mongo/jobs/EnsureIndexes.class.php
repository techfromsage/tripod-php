<?php

namespace Tripod\Mongo\Jobs;

/**
 * Class EnsureIndexes
 * @package Tripod\Mongo\Jobs
 */
class EnsureIndexes extends JobBase {

    const STORENAME_KEY  = 'storeName';
    const REINDEX_KEY    = 'reindex';

    /**
     * Runs the EnsureIndexes Job
     * @throws \Exception
     */
    public function perform()
    {
        try
        {
            $this->debugLog("[JOBID " . $this->job->payload['id'] . "] EnsureIndexes::perform() start");

            $timer = new \Tripod\Timer();
            $timer->start();

            $this->validateArgs();

            \Tripod\Mongo\Config::setConfig($this->args[self::TRIPOD_CONFIG_KEY]);

            $this->getIndexUtils()->ensureIndexes(
                $this->args[self::REINDEX_KEY],
                $this->args[self::STORENAME_KEY],
                true // always create indexes in the background
            );

            $timer->stop();
            // stat time taken to process job, from time it was picked up
            $this->getStat()->timer(MONGO_QUEUE_ENSURE_INDEXES_SUCCESS,$timer->result());
            $this->debugLog("[JOBID " . $this->job->payload['id'] . "] EnsureIndexes::perform() done in {$timer->result()}ms");
        }
        catch(\Exception $e)
        {
            $this->getStat()->increment(MONGO_QUEUE_ENSURE_INDEXES_FAIL);
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * Validate args for EnsureIndexesOperation
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array(self::TRIPOD_CONFIG_KEY, self::STORENAME_KEY, self::REINDEX_KEY);
    }

    /**
     * @return \Tripod\Mongo\IndexUtils
     */
    protected function getIndexUtils()
    {
        return new \Tripod\Mongo\IndexUtils();
    }

    public function createJob($storeName, $reindex, $queueName=null)
    {
        if(!$queueName)
        {
            $queueName = \Tripod\Mongo\Config::getEnsureIndexesQueueName();
        }
        elseif(strpos($queueName, \Tripod\Mongo\Config::getEnsureIndexesQueueName()) === false)
        {
            $queueName = \Tripod\Mongo\Config::getEnsureIndexesQueueName() . '::' . $queueName;
        }

        $data = array(
            self::STORENAME_KEY=>$storeName,
            self::REINDEX_KEY=>$reindex,
            self::TRIPOD_CONFIG_KEY=>\Tripod\Mongo\Config::getConfig()
        );

        $this->submitJob($queueName,get_class($this),$data);
    }
}
