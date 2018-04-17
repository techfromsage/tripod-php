<?php

namespace Tripod\Mongo\Jobs;

/**
 * Class EnsureIndexes
 * @package Tripod\Mongo\Jobs
 */
class EnsureIndexes extends JobBase
{

    const STORENAME_KEY  = 'storeName';
    const REINDEX_KEY    = 'reindex';
    const BACKGROUND_KEY = 'background';

    protected $mandatoryArgs = [self::STORENAME_KEY, self::REINDEX_KEY, self::BACKGROUND_KEY];
    protected $configRequired = true;

    /**
     * Runs the EnsureIndexes Job
     * @throws \Exception
     */
    public function perform()
    {
        try {
            $this->debugLog('Ensuring indexes for tenant=' . $this->args[self::STORENAME_KEY]. ', reindex=' . $this->args[self::REINDEX_KEY] . ', background=' . $this->args[self::BACKGROUND_KEY]);

            $this->getIndexUtils()->ensureIndexes(
                $this->args[self::REINDEX_KEY],
                $this->args[self::STORENAME_KEY],
                $this->args[self::BACKGROUND_KEY]
            );

            // stat time taken to process job, from time it was picked up
            $this->getStat()->timer(MONGO_QUEUE_ENSURE_INDEXES_SUCCESS, $this->timer->result());
        } catch (\Exception $e) {
            $this->getStat()->increment(MONGO_QUEUE_ENSURE_INDEXES_FAIL);
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * This method is use to schedule an EnsureIndexes job.
     *
     * @param string $storeName
     * @param booelan $reindex
     * @param string $queueName
     */
    public function createJob($storeName, $reindex, $background, $queueName = null)
    {
        if (!$queueName) {
            $queueName = \Tripod\Mongo\Config::getEnsureIndexesQueueName();
        } elseif (strpos($queueName, \Tripod\Mongo\Config::getEnsureIndexesQueueName()) === false) {
            $queueName = \Tripod\Mongo\Config::getEnsureIndexesQueueName() . '::' . $queueName;
        }

        $data = array(
            self::STORENAME_KEY => $storeName,
            self::REINDEX_KEY => $reindex,
            self::BACKGROUND_KEY => $background,
            self::TRIPOD_CONFIG_KEY => \Tripod\Mongo\Config::getConfig()
        );

        $this->submitJob($queueName, get_class($this), $data);
    }

    /**
     * @return \Tripod\Mongo\IndexUtils
     */
    protected function getIndexUtils()
    {
        return new \Tripod\Mongo\IndexUtils();
    }
}
