<?php

namespace Tripod\Mongo\Jobs;
/**
 * Todo: How to inject correct stat class... :-S
 */
abstract class JobBase extends \Tripod\Mongo\DriverBase
{
    private $tripod;
    const TRIPOD_CONFIG_KEY = 'tripodConfig';
    const QUEUE_KEY = 'queue';

    /**
     * For mocking
     * @param string $storeName
     * @param string $podName
     * @return \Tripod\Mongo\Driver
     */
    protected function getTripod($storeName,$podName) {
        if ($this->tripod == null) {
            $this->tripod = new \Tripod\Mongo\Driver(
                $podName,
                $storeName,
                array(
                    'stat'=>$this->getStat(),
                    'readPreference'=>\MongoClient::RP_PRIMARY // important: make sure we always read from the primary
                )
            );
        }
        return $this->tripod;
    }

    /**
     * Make sure each job considers how to validate it's args
     * @return array
     */
    protected abstract function getMandatoryArgs();

    /**
     * Validate the arguments for this job
     * @throws \Exception
     */
    protected function validateArgs()
    {
        foreach ($this->getMandatoryArgs() as $arg)
        {
            if (!isset($this->args[$arg]))
            {
                $message = "Argument $arg was not present in supplied job args for job ".get_class($this);
                $this->errorLog($message);
                throw new \Exception($message);
            }
        }
    }

    /**
     * @param string $message
     * @param mixed $params
     */
    public function debugLog($message, $params = null)
    {
        parent::debugLog("[PID ".getmypid()."] ".$message, $params);
    }

    /**
     * @param string $message
     * @param mixed $params
     */
    public function errorLog($message, $params = null)
    {
        parent::errorLog("[PID ".getmypid()."] ".$message, $params);
    }


    /**
     * @param string $queueName
     * @param string $class
     * @param array $data
     */
    protected function submitJob($queueName, $class, Array $data)
    {
        \Resque::enqueue($queueName, $class, $data);
    }
}

