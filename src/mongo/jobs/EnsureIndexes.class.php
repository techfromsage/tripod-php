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
        $this->debugLog('Ensuring indexes for tenant=' . $this->args[self::STORENAME_KEY]. ', reindex=' . $this->args[self::REINDEX_KEY] . ', background=' . $this->args[self::BACKGROUND_KEY]);

        $this->getIndexUtils()->ensureIndexes(
            $this->args[self::REINDEX_KEY],
            $this->args[self::STORENAME_KEY],
            $this->args[self::BACKGROUND_KEY]
        );
    }

    /**
     * Stat string for successful job timer
     *
     * @return string
     */
    protected function getStatTimerSuccessKey()
    {
        return MONGO_QUEUE_ENSURE_INDEXES_SUCCESS;
    }

    /**
     * Stat string for failed job increment
     *
     * @return string
     */
    protected function getStatFailureIncrementKey()
    {
        return MONGO_QUEUE_ENSURE_INDEXES_FAIL;
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
