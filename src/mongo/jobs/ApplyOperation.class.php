<?php

namespace Tripod\Mongo\Jobs;

use Tripod\Mongo\JobGroup;


/**
 * Class ApplyOperation
 * @package Tripod\Mongo\Jobs
 */
class ApplyOperation extends JobBase {

    const SUBJECTS_KEY = 'subjects';
    const TRACKING_KEY = 'batchId';

    /**
     * Run the ApplyOperation job
     * @throws \Exception
     */
    public function perform()
    {
        try {
            $this->debugLog("[JOBID " . $this->job->payload['id'] . "] ApplyOperation::perform() start");

            $timer = new \Tripod\Timer();
            $timer->start();

            $this->validateArgs();

            $statsConfig = array();
            if(isset($this->args['statsConfig']))
            {
                $statsConfig['statsConfig'] = $this->args['statsConfig'];
            }

            // set the config to what is received
            \Tripod\Mongo\Config::setConfig($this->args[self::TRIPOD_CONFIG_KEY]);

            $this->getStat()->increment(MONGO_QUEUE_APPLY_OPERATION_JOB . '.' . SUBJECT_COUNT, count($this->args[self::SUBJECTS_KEY]));

            foreach ($this->args[self::SUBJECTS_KEY] as $subject) {
                $opTimer = new \Tripod\Timer();
                $opTimer->start();

                $impactedSubject = $this->createImpactedSubject($subject);
                $impactedSubject->update();

                $opTimer->stop();
                // stat time taken to perform operation for the given subject
                $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION.'.'.$subject['operation'], $opTimer->result());
            }


            $timer->stop();
            // stat time taken to process job, from time it was picked up
            $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION_SUCCESS,$timer->result());

            if (isset($this->args[self::TRACKING_KEY])) {
                $jobGroup = new JobGroup($this->args['storeName'], $this->args[self::TRACKING_KEY]);
                $jobCount = $jobGroup->incrementJobCount(-1);
                if ($jobCount <= 0) {
                    // @todo Replace this with ObjectId->getTimestamp() if we upgrade Mongo driver to 1.2
                    $timestamp = new \MongoDB\BSON\UTCDateTime(hexdec(substr($jobGroup->getId(), 0, 8)) * 1000);
                    $tripod = $this->getTripod($this->args['storeName'], $this->args['podName']);
                    $count = 0;
                    foreach ($this->args['specId'] as $specId) {
                        switch ($this->args['operation']) {
                            case \OP_VIEWS:
                                $count += $tripod->getTripodViews()->deleteViewsByViewId($specId, $timestamp);
                                break;
                            case \OP_TABLES:
                                $count += $tripod->getTripodTables()->deleteTableRowsByTableId($specId, $timestamp);
                                break;
                            case \OP_SEARCH:
                                $searchProvider = new \Tripod\Mongo\MongoSearchProvider($tripod);
                                $count += $searchProvider->deleteSearchDocumentsByTypeId($specId, $timestamp);
                                break;
                        }
                    }
                    $this->infoLog(
                        '[JobGroupId ' . $jobGroup->getId()->__toString() . '] composite cleanup for ' .
                        $this->args['operation'] . ' removed ' . $count . ' stale composite documents'
                    );
                }
            }
            $this->debugLog("[JOBID " . $this->job->payload['id'] . "] ApplyOperation::perform() done in {$timer->result()}ms");
        } catch (\Exception $e) {
            $this->getStat()->increment(MONGO_QUEUE_APPLY_OPERATION_FAIL);
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * @param \Tripod\Mongo\ImpactedSubject[] $subjects
     * @param string|null $queueName
     * @param array $otherData
     */
    public function createJob(Array $subjects, $queueName=null,$otherData=array())
    {
        if(!$queueName)
        {
            $queueName = \Tripod\Mongo\Config::getApplyQueueName();
        }
        elseif(strpos($queueName, \Tripod\Mongo\Config::getApplyQueueName()) === false)
        {
            $queueName = \Tripod\Mongo\Config::getApplyQueueName() . '::' . $queueName;
        }

        $data = array(
            self::SUBJECTS_KEY=>array_map(function(\Tripod\Mongo\ImpactedSubject $subject) { return $subject->toArray(); }, $subjects),
            self::TRIPOD_CONFIG_KEY=>\Tripod\Mongo\Config::getConfig()
        );

        $this->submitJob($queueName,get_class($this),array_merge($otherData,$data));
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
     * Validate args for ApplyOperation
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array(self::TRIPOD_CONFIG_KEY,self::SUBJECTS_KEY);
    }
}
