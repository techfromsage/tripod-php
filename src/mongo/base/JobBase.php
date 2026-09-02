<?php

declare(strict_types=1);

namespace Tripod\Mongo\Jobs;

use MongoDB\Driver\ReadPreference;
use Psr\Log\LoggerInterface;
use Resque\Job\Job;
use Resque\Job\Status;
use Resque\JobHandler;
use Resque\Resque;
use Tripod\Config;
use Tripod\Exceptions\Exception;
use Tripod\Exceptions\JobException;
use Tripod\ITripodConfigSerializer;
use Tripod\ITripodStat;
use Tripod\LoggerTrait;
use Tripod\Mongo\Driver;
use Tripod\Mongo\DriverBase;
use Tripod\Mongo\IConfigInstance;
use Tripod\Timer;
use Tripod\TripodStatFactory;

abstract class JobBase extends Job
{
    use LoggerTrait;

    public const TRIPOD_CONFIG_KEY = 'tripodConfig';

    public const TRIPOD_CONFIG_GENERATOR = 'tripodConfigGenerator';

    public const QUEUE_KEY = 'queue';

    /**
     * @var string[]
     */
    protected $mandatoryArgs = [];

    /**
     * @var bool
     */
    protected $configRequired = false;

    protected ?ITripodStat $stat = null;

    protected array $statsConfig = [];

    protected ?IConfigInstance $tripodConfig = null;

    protected ?Timer $timer = null;

    private ?Driver $tripod = null;

    public function setUp(): void
    {
        $this->debugLog(
            '[JOBID ' . $this->job->payload['id'] . '] ' . get_class($this) . '::perform() start'
        );

        $this->timer = new Timer();
        $this->timer->start();

        $this->setStatsConfig();

        if ($this->configRequired) {
            $this->setTripodConfig();
        }
    }

    public function tearDown(): void
    {
        $timer = $this->timer;
        if ($timer === null) {
            return;
        }

        // stat time taken to process item, from time it was created (queued)
        $timer->stop();
        $this->debugLog(
            '[JOBID ' . $this->job->payload['id'] . '] ' . get_class($this)
                . sprintf('::perform() done in %sms', $timer->result())
        );
        $this->getStat()->timer($this->getStatTimerSuccessKey(), $timer->result());
    }

    /**
     * The main method of the job.
     */
    abstract public function perform(): void;

    /**
     * Called in every job prior to perform().
     *
     * @param JobHandler|\Resque_Job $job The job instance
     */
    public static function beforePerform($job): void
    {
        $instance = $job->getInstance();
        if (!$instance instanceof self) {
            return;
        }

        $instance->validateArgs();
    }

    /**
     * Resque event when a job failures.
     *
     * @param \Exception|\Throwable  $e   Exception or Error
     * @param JobHandler|\Resque_Job $job The failed job
     */
    public static function onFailure($e, $job): void
    {
        $failedJob = $job->getInstance();
        if (!$failedJob instanceof self) {
            return;
        }

        $failedJob->errorLog('Caught exception in ' . static::class . ': ' . $e->getMessage());
        $failedJob->getStat()->increment($failedJob->getStatFailureIncrementKey());
    }

    public static function getLogger(): LoggerInterface
    {
        return DriverBase::getLogger();
    }

    public function getStat(): ITripodStat
    {
        if ($this->statsConfig === []) {
            $this->getStatsConfig();
        }

        $stat = $this->stat;
        if ($stat == null) {
            $stat = $this->getStatFromStatFactory();
            $this->setStat($stat);
        }

        return $stat;
    }

    public function setStat(ITripodStat $stat): void
    {
        $this->stat = $stat;
    }

    /**
     * Gets the stats config for the job.
     */
    public function getStatsConfig(): array
    {
        if ($this->statsConfig === []) {
            $this->setStatsConfig();
        }

        return $this->statsConfig;
    }

    /**
     * For mocking out the creation of stat objects.
     */
    protected function getStatFromStatFactory(): ITripodStat
    {
        return TripodStatFactory::create($this->statsConfig);
    }

    /**
     * Stat string for successful job timer.
     */
    abstract protected function getStatTimerSuccessKey(): string;

    /**
     * Stat string for failed job increment.
     */
    abstract protected function getStatFailureIncrementKey(): string;

    protected function getTripod(string $storeName, string $podName, array $opts = []): Driver
    {
        $this->getTripodConfig();

        $opts = array_merge(
            $opts,
            [
                'stat' => $this->getStat(),
                'readPreference' => ReadPreference::PRIMARY, // important: make sure we always read from the primary
            ]
        );
        if ($this->tripod == null) {
            $this->tripod = new Driver(
                $podName,
                $storeName,
                $opts
            );
        }

        return $this->tripod;
    }

    /**
     * Make sure each job considers how to validate its args.
     */
    protected function getMandatoryArgs(): array
    {
        return $this->mandatoryArgs;
    }

    /**
     * Validate the arguments for this job.
     *
     * @throws Exception
     */
    protected function validateArgs(): void
    {
        $message = null;
        foreach ($this->getMandatoryArgs() as $arg) {
            if (!isset($this->args[$arg])) {
                $message = sprintf('Argument %s was not present in supplied job args for job ', $arg) . get_class($this);
                $this->errorLog($message);

                throw new Exception($message);
            }
        }

        if ($this->configRequired) {
            $this->ensureConfig();
        }
    }

    protected function ensureConfig(): void
    {
        if (!isset($this->args[self::TRIPOD_CONFIG_KEY]) && !isset($this->args[self::TRIPOD_CONFIG_GENERATOR])) {
            $message = 'Argument ' . self::TRIPOD_CONFIG_KEY . ' or ' . self::TRIPOD_CONFIG_GENERATOR
                . ' was not present in supplied job args for job ' . get_class($this);
            $this->errorLog($message);

            throw new Exception($message);
        }
    }

    /**
     * @param string               $queueName     Queue name
     * @param string               $class         Class name
     * @param array<string, mixed> $data          Job arguments
     * @param int                  $retryAttempts If queue fails, retry x times before throwing an exception
     *
     * @return string A tracking token for the submitted job
     *
     * @throws JobException If there is a problem queuing the job
     */
    protected function submitJob(string $queueName, string $class, array $data, int $retryAttempts = 5): string
    {
        // @see https://github.com/chrisboulton/php-resque/issues/228, when this PR is merged we can stop tracking the status in this way
        try {
            if (isset($data[self::TRIPOD_CONFIG_GENERATOR]) && $data[self::TRIPOD_CONFIG_GENERATOR]) {
                $data[self::TRIPOD_CONFIG_GENERATOR] = $this->serializeConfig($data[self::TRIPOD_CONFIG_GENERATOR]);
            }

            $token = $this->enqueue($queueName, $class, $data);
            if (!$token || !$this->hasJobStatus($token)) {
                $this->errorLog(sprintf('Could not retrieve status for queued %s job - job %s failed to %s', $class, $token, $queueName));

                throw new Exception(sprintf('Could not retrieve status for queued job - job %s failed to %s', $token, $queueName));
            }

            $this->debugLog(sprintf('Queued %s job with %s to %s', $class, $token, $queueName));

            return $token;
        } catch (\Exception $e) {
            if ($retryAttempts > 0) {
                sleep(1); // back off for 1 sec
                $this->warningLog(sprintf('Exception queuing %s job - %s, retrying %d times', $class, $e->getMessage(), $retryAttempts));

                return $this->submitJob($queueName, $class, $data, --$retryAttempts);
            }

            $this->errorLog(sprintf('Exception queuing %s job - %s', $class, $e->getMessage()));

            throw new JobException('Exception queuing job  - ' . $e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Actually enqueues the job with Resque. Returns a tracking token. For mocking.
     *
     * @param array<string, mixed> $data
     *
     * @return false|string
     */
    protected function enqueue(string $queueName, string $class, array $data)
    {
        $token = Resque::enqueue($queueName, $class, $data, true);

        // Resque's docblock claims bool|string, but a job ID string is returned whenever
        // the job is created; false only when a beforeEnqueue hook cancels creation.
        return is_string($token) ? $token : false;
    }

    /**
     * Given a token, return the job status.
     */
    protected function hasJobStatus(string $token): bool
    {
        $status = new Status($token);

        return !empty($status->get());
    }

    /**
     * Take a Tripod Config Serializer and return a config array.
     *
     * @param mixed $configSerializer An object that implements ITripodConfigSerializer
     */
    protected function serializeConfig($configSerializer): array
    {
        if ($configSerializer instanceof ITripodConfigSerializer) {
            return $configSerializer->serialize();
        }

        if (is_array($configSerializer)) {
            return $configSerializer;
        }

        throw new \InvalidArgumentException(
            '$configSerializer must an ITripodConfigSerializer or array'
        );
    }

    /**
     * Deserialize a tripodConfigGenerator argument to a Tripod Config object.
     *
     * @param array $config The serialized Tripod config
     */
    protected function deserializeConfig(array $config): IConfigInstance
    {
        Config::setConfig($config);

        return Config::getInstance();
    }

    /**
     * For mocking.
     */
    protected function getConfigInstance(): IConfigInstance
    {
        return Config::getInstance();
    }

    /**
     * Sets the Tripod config for the job.
     */
    protected function setTripodConfig(): void
    {
        if (isset($this->args[self::TRIPOD_CONFIG_GENERATOR])) {
            $config = $this->args[self::TRIPOD_CONFIG_GENERATOR];
        } else {
            $config = $this->args[self::TRIPOD_CONFIG_KEY];
        }

        $this->tripodConfig = $this->deserializeConfig($config);
    }

    /**
     * Returns the Tripod config required by the job.
     */
    protected function getTripodConfig(): IConfigInstance
    {
        if ($this->tripodConfig === null) {
            $this->ensureConfig();
            $this->setTripodConfig();
        }

        $tripodConfig = $this->tripodConfig;
        if ($tripodConfig === null) {
            throw new \RuntimeException('Tripod config could not be initialised for job');
        }

        return $tripodConfig;
    }

    /**
     * Tripod options to pass between jobs.
     *
     * @return array<string, mixed>
     */
    protected function getTripodOptions(): array
    {
        $statsConfig = $this->getStatsConfig();
        $options = [];
        if (!empty($statsConfig)) {
            $options['statsConfig'] = $statsConfig;
        }

        return $options;
    }

    /**
     * Convenience method to pass config to job data.
     *
     * @return array<string, mixed>
     */
    protected function generateConfigJobArgs(): array
    {
        $configInstance = $this->getConfigInstance();
        $args = [];
        $config = $configInstance->serialize();

        if (isset($config['class'])) {
            $args[self::TRIPOD_CONFIG_GENERATOR] = $config;
        } else {
            $args[self::TRIPOD_CONFIG_KEY] = $config;
        }

        return $args;
    }

    /**
     * Sets the stats config for the job.
     */
    private function setStatsConfig(): void
    {
        if (isset($this->args['statsConfig'])) {
            $this->statsConfig = $this->args['statsConfig'];
        }
    }
}
