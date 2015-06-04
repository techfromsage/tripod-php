<?php

namespace Tripod\Mongo\Jobs;

/**
 * Class ApplyOperation
 * @package Tripod\Mongo\Jobs
 */
class ApplyOperation extends JobBase {
    /**
     * Run the ApplyOperation job
     * @throws \Exception
     */
    public function perform()
    {
        try
        {
            $this->debugLog("ApplyOperation::perform() start");

            $timer = new \Tripod\Timer();
            $timer->start();

            $this->validateArgs();

            // set the config to what is received
            \Tripod\Mongo\Config::setConfig($this->args["tripodConfig"]);

            foreach($this->args['subjects'] as $subject)
            {
                $impactedSubject = $this->createImpactedSubject($subject);
                $impactedSubject->update();
            }


            $timer->stop();
            // stat time taken to process item, from time it was created (queued)
            $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION_SUCCESS,$timer->result());

            $this->debugLog("ApplyOperation::perform() done in {$timer->result()}ms");
        }
        catch (\Exception $e)
        {
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * @param \Tripod\Mongo\ImpactedSubject[] $subjects
     * @param string|null $queueName
     */
    public function createJob(Array $subjects, $queueName=null)
    {
        if(!$queueName)
        {
            $queueName = \Tripod\Mongo\Config::getApplyQueueName();
        }

        $subjectArray = array();
        foreach($subjects as $subject)
        {
            $subjectArray[] = $subject->toArray();
        }
        $data = array(
            "subjects"=>$subjectArray,
            "tripodConfig"=>\Tripod\Mongo\Config::getConfig()
        );

        $this->submitJob($queueName,get_class($this),$data);
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
        return array("tripodConfig","subjects");
    }
}
