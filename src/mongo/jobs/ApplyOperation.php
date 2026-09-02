<?php

declare(strict_types=1);

namespace Tripod\Mongo\Jobs;

use MongoDB\BSON\ObjectId;
use MongoDB\BSON\UTCDateTime;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\JobGroup;
use Tripod\Mongo\MongoSearchProvider;
use Tripod\Timer;

class ApplyOperation extends JobBase
{
    public const SUBJECTS_KEY = 'subjects';

    public const TRACKING_KEY = 'batchId';

    protected $configRequired = true;

    protected $mandatoryArgs = [self::SUBJECTS_KEY];

    /**
     * Run the ApplyOperation job.
     *
     * @throws \Exception
     */
    public function perform(): void
    {
        $this->getStat()->increment(
            MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT,
            count($this->args[self::SUBJECTS_KEY])
        );

        foreach ($this->args[self::SUBJECTS_KEY] as $subject) {
            $opTimer = new Timer();
            $opTimer->start();

            $impactedSubject = $this->createImpactedSubject($subject);
            $impactedSubject->update();

            $opTimer->stop();
            // stat time taken to perform operation for the given subject
            $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION . '.' . $subject['operation'], $opTimer->result());

            /*
             * ApplyOperation jobs can either apply to a single resource (e.g. 'create composite for the given
             * resource uri) or for a specification id (i.e. regenerate all of the composites defined by the
             * specification).  For the latter, we need to keep track of how many jobs have run so we can clean
             * up any stale composite documents when completed.  The TRACKING_KEY value will be the JobGroup id.
             */
            if (isset($this->args[self::TRACKING_KEY])) {
                $jobGroup = $this->getJobGroup($subject['storeName'], $this->args[self::TRACKING_KEY]);
                $jobCount = $jobGroup->incrementJobCount(-1);
                if ($jobCount <= 0) {
                    $timestamp = new UTCDateTime($jobGroup->getId()->getTimestamp() * 1000);
                    $tripod = $this->getTripod($subject['storeName'], $subject['podName']);
                    $count = 0;
                    foreach ($subject['specTypes'] as $specId) {
                        switch ($subject['operation']) {
                            case \OP_VIEWS:
                                $count += $tripod->getTripodViews()->deleteViewsByViewId($specId, $timestamp);

                                break;

                            case \OP_TABLES:
                                $count += $tripod->getTripodTables()->deleteTableRowsByTableId($specId, $timestamp);

                                break;

                            case \OP_SEARCH:
                                $searchProvider = $this->getSearchProvider($tripod);
                                $count += $searchProvider->deleteSearchDocumentsByTypeId($specId, $timestamp);

                                break;
                        }
                    }

                    $this->infoLog(
                        '[JobGroupId ' . $jobGroup->getId()->__toString() . '] composite cleanup for '
                            . $subject['operation'] . ' removed ' . $count . ' stale composite documents'
                    );
                }
            }
        }
    }

    /**
     * @param ImpactedSubject[] $subjects
     */
    public function createJob(array $subjects, ?string $queueName = null, array $otherData = []): void
    {
        $configInstance = $this->getConfigInstance();
        if (!$queueName) {
            $queueName = $configInstance::getApplyQueueName();
        } elseif (strpos($queueName, $configInstance::getApplyQueueName()) === false) {
            $queueName = $configInstance::getApplyQueueName() . '::' . $queueName;
        }

        $data = [
            self::SUBJECTS_KEY => array_map(
                fn (ImpactedSubject $subject): array => $subject->toArray(),
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
     * Stat string for successful job timer.
     */
    protected function getStatTimerSuccessKey(): string
    {
        return MONGO_QUEUE_APPLY_OPERATION_SUCCESS;
    }

    /**
     * Stat string for failed job increment.
     */
    protected function getStatFailureIncrementKey(): string
    {
        return MONGO_QUEUE_APPLY_OPERATION_FAIL;
    }

    /**
     * For mocking.
     */
    protected function createImpactedSubject(array $args): ImpactedSubject
    {
        return new ImpactedSubject(
            $args['resourceId'],
            $args['operation'],
            $args['storeName'],
            $args['podName'],
            $args['specTypes']
        );
    }

    /**
     * For mocking.
     *
     * @param string          $storeName   Tripod store (database) name
     * @param ObjectId|string $trackingKey JobGroup ID
     */
    protected function getJobGroup(string $storeName, $trackingKey): JobGroup
    {
        return new JobGroup($storeName, $trackingKey);
    }

    /**
     * For mocking.
     */
    protected function getSearchProvider(Driver $tripod): MongoSearchProvider
    {
        return new MongoSearchProvider($tripod);
    }
}
