<?php

namespace Tripod\Mongo\Jobs;

use Tripod\Mongo\JobGroup;
use Tripod\Mongo\Driver;
use Tripod\ITripodConfigSerializer;

/**
 * Class ApplyOperation
 * @package Tripod\Mongo\Jobs
 */
class ApplyOperation extends JobBase
{

    const SUBJECTS_KEY = 'subjects';
    const TRACKING_KEY = 'batchId';

    protected $configRequired = true;
    protected $mandatoryArgs = [self::SUBJECTS_KEY];

    /**
     * Run the ApplyOperation job
     * @throws \Exception
     */
    public function perform()
    {
        $this->getStat()->increment(
            MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT,
            count($this->args[self::SUBJECTS_KEY])
        );

        foreach ($this->args[self::SUBJECTS_KEY] as $subject) {
            $opTimer = new \Tripod\Timer();
            $opTimer->start();

            $impactedSubject = $this->createImpactedSubject($subject);
            $impactedSubject->update();

            $opTimer->stop();
            // stat time taken to perform operation for the given subject
            $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION.'.'.$subject['operation'], $opTimer->result());

            /**
             * ApplyOperation jobs can either apply to a single resource (e.g. 'create composite for the given
             * resource uri) or for a specification id (i.e. regenerate all of the composites defined by the
             * specification).  For the latter, we need to keep track of how many jobs have run so we can clean
             * up any stale composite documents when completed.  The TRACKING_KEY value will be the JobGroup id.
             */
            if (isset($this->args[self::TRACKING_KEY])) {
                $jobGroup = $this->getJobGroup($subject['storeName'], $this->args[self::TRACKING_KEY]);
                $jobCount = $jobGroup->incrementJobCount(-1);
                if ($jobCount <= 0) {
                    // @todo Replace this with ObjectId->getTimestamp() if we upgrade Mongo driver to 1.2
                    $timestamp = new \MongoDB\BSON\UTCDateTime(hexdec(substr($jobGroup->getId(), 0, 8)) * 1000);
                    $tripod = $this->getTripod($subject['storeName'], $subject['podName']);
                    $count = 0;
                    foreach ($subject['specTypes'] as $specId) {
                        switch ($subject['operation']) {
                            case \OP_VIEWS:
                                $count += $tripod->getComposite(\OP_VIEWS)->deleteViewsByViewId($specId, $timestamp);
                                break;
                            case \OP_TABLES:
                                $count += $tripod->getComposite(\OP_TABLES)->deleteTableRowsByTableId($specId, $timestamp);
                                break;
                            case \OP_SEARCH:
                                $searchProvider = $this->getSearchProvider($tripod);
                                $count += $searchProvider->deleteSearchDocumentsByTypeId($specId, $timestamp);
                                break;
                        }
                    }
                    $this->infoLog(
                        '[JobGroupId ' . $jobGroup->getId()->__toString() . '] composite cleanup for ' .
                        $subject['operation'] . ' removed ' . $count . ' stale composite documents'
                    );
                }
            }
        }
    }

    /**
     * Stat string for successful job timer
     *
     * @return string
     */
    protected function getStatTimerSuccessKey()
    {
        return MONGO_QUEUE_APPLY_OPERATION_SUCCESS;
    }

    /**
     * Stat string for failed job increment
     *
     * @return string
     */
    protected function getStatFailureIncrementKey()
    {
        return MONGO_QUEUE_APPLY_OPERATION_FAIL;
    }

    /**
     * @param \Tripod\Mongo\ImpactedSubject[] $subjects
     * @param string|null $queueName
     * @param array $otherData
     */
    public function createJob(array $subjects, $queueName = null, $otherData = [])
    {
        $configInstance = $this->getConfigInstance();
        if (!$queueName) {
            $queueName = $configInstance::getApplyQueueName();
        } elseif (strpos($queueName, $configInstance::getApplyQueueName()) === false) {
            $queueName = $configInstance::getApplyQueueName() . '::' . $queueName;
        }

        $data = [
            self::SUBJECTS_KEY => array_map(
                function (\Tripod\Mongo\ImpactedSubject $subject) {
                    return $subject->toArray();
                },
                $subjects
            ),
        ];

        $data = array_merge(
            $this->generateConfigJobArgs(),
            $data
        );

        $this->submitJob($queueName, get_class($this), array_merge($otherData, $data));
    }

    /**
     * For mocking
     * @param array $args
     * @return \Tripod\Mongo\ImpactedSubject
     */
    protected function createImpactedSubject(array $args)
    {
        return new \Tripod\Mongo\ImpactedSubject(
            $args["resourceId"],
            $args["operation"],
            $args["storeName"],
            $args["podName"],
            $args["specTypes"]
        );
    }

    /**
     * For mocking
     *
     * @param string                        $storeName   Tripod store (database) name
     * @param string|\MongoDB\BSON\ObjectId $trackingKey JobGroup ID
     * @return JobGroup
     */
    protected function getJobGroup($storeName, $trackingKey)
    {
        return new JobGroup($storeName, $trackingKey);
    }

    /**
     * For mocking
     *
     * @param Driver $tripod
     * @return \Tripod\Mongo\MongoSearchProvider
     */
    protected function getSearchProvider(Driver $tripod)
    {
        return new \Tripod\Mongo\MongoSearchProvider($tripod);
    }
}
