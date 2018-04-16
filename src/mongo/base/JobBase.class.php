<?php

namespace Tripod\Mongo\Jobs;
use Tripod\Exceptions\Exception;
use \Tripod\Exceptions\ConfigException;
use Tripod\Exceptions\JobException;
use \MongoDB\Driver\ReadPreference;

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
     * @param array $opts
     * @return \Tripod\Mongo\Driver
     */
    protected function getTripod($storeName,$podName,$opts=array()) {
        $opts = array_merge($opts,array(
            'stat'=>$this->getStat(),
            'readPreference' => ReadPreference::RP_PRIMARY // important: make sure we always read from the primary
        ));
        if ($this->tripod == null) {
            $this->tripod = new \Tripod\Mongo\Driver(
                $podName,
                $storeName,
                $opts
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
        parent::debugLog($message, $params);
    }

    /**
     * @param string $message
     * @param mixed $params
     */
    public function errorLog($message, $params = null)
    {
        parent::errorLog($message, $params);
    }


    /**
     * @param string $queueName
     * @param string $class
     * @param array $data
     * @param int $retryAttempts if queue fails, retry x times before throwing an exception
     * @return a tracking token for the submitted job
     * @throws JobException if there is a problem queuing the job
     */
    protected function submitJob($queueName, $class, array $data, $retryAttempts = 5)
    {
        // @see https://github.com/chrisboulton/php-resque/issues/228, when this PR is merged we can stop tracking the status in this way
        try {
            if (isset($data[self::TRIPOD_CONFIG_KEY])) {
                $data[self::TRIPOD_CONFIG_KEY] = $this->cacheConfig($data[self::TRIPOD_CONFIG_KEY]);
            }
            $token = $this->enqueue($queueName, $class, $data);
            if (!$this->getJobStatus($token)) {
                $this->errorLog("Could not retrieve status for queued $class job - job $token failed to $queueName");
                throw new \Exception("Could not retrieve status for queued job - job $token failed to $queueName");
            } else {
                $this->debugLog("Queued $class job with $token to $queueName");
                return $token;
            }
        } catch (\Exception $e) {
            if ($retryAttempts > 0) {
                sleep(1); // back off for 1 sec
                $this->warningLog("Exception queuing $class job - {$e->getMessage()}, retrying $retryAttempts times");
                return $this->submitJob($queueName, $class, $data, --$retryAttempts);
            } else {
                $this->errorLog("Exception queuing $class job - {$e->getMessage()}");
                throw new JobException("Exception queuing job  - {$e->getMessage()}", $e->getCode(), $e);
            }
        }
    }

    /**
     * Actually enqueues the job with Resque. Returns a tracking token. For mocking.
     * @param string $queueName
     * @param string $class
     * @param mixed $data
     * @internal param bool|\Tripod\Mongo\Jobs\false $tracking
     * @return string
     */
    protected function enqueue($queueName, $class, $data)
    {
        return \Resque::enqueue($queueName, $class, $data, true);
    }

    /**
     * Given a token, return the job status. For mocking
     * @param string $token
     * @return mixed
     */
    protected function getJobStatus($token)
    {
        $status = new \Resque_Job_Status($token);
        return $status->get();
    }

    /**
     * @return \Tripod\ITripodStat
     */
    public function getStat()
    {
        if((!isset($this->statsConfig) || empty($this->statsConfig)) && isset($this->args['statsConfig']))
        {
            $this->statsConfig = $this->args['statsConfig'];
        }
        return parent::getStat();
    }

    protected function cacheConfig($config)
    {
        if (empty($config)) {
            throw new ConfigException('Empty config sent');
        }
        $key = null;
        if (is_array($config)) {
            $key = self::TRIPOD_CONFIG_KEY . ':' . md5(json_encode($config));
            \Resque::redis()->set($key);
        } elseif (is_string($config)) {
            if (strpos($config, self::TRIPOD_CONFIG_KEY . ':') === 0) {
                $key = $config;
            }
        }

        $cachedConfig = $this->getConfig($key);
        if (empty($cachedConfig) && is_array($config)) {
            \Resque::redis()->set($key);
            try {
                $cachedConfig = $this->cacheConfig($key);
                return $cachedConfig;
            } catch (ConfigException $e) {
                return $config;
            }
        } elseif (empty($cachedConfig)) {
            throw new ConfigException('Empty config or expired from cache');
        }
        return $key;
    }

    protected function getConfig($config)
    {
        if (is_array($config)) {
            return $config;
        }
        if (is_string($config)) {
            if (strpos($config, self::TRIPOD_CONFIG_KEY . ':') === 0) {
                return \Resque::redis()->get($config);
            }
            $jsonConfig = json_decode($config, true);
            if ($jsonConfig) {
                return $jsonConfig;
            }
        }
        return null;
    }

}

