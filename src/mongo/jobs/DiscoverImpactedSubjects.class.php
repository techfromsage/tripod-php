<?php

namespace Tripod\Mongo\Jobs;

use \Tripod\Mongo\Config;
/**
 * Class DiscoverImpactedSubjects
 * @package Tripod\Mongo\Jobs
 */
class DiscoverImpactedSubjects extends JobBase {

    /**
     * @var ApplyOperation
     */
    protected $applyOperation;

    /**
     * Run the DiscoverImpactedSubjects job
     * @throws \Exception
     */
    public function perform()
    {
        try
        {

            $this->debugLog("DiscoverImpactedSubjects::perform() start");

            $timer = new \Tripod\Timer();
            $timer->start();

            $this->validateArgs();

            // set the config to what is received
            \Tripod\Mongo\Config::setConfig($this->args["tripodConfig"]);

            $tripod = $this->getTripod($this->args["storeName"],$this->args["podName"]);

            $operations = $this->args['operations'];
            $modifiedSubjects = array();

            $subjectsAndPredicatesOfChange = $this->args['changes'];

            foreach($operations as $op)
            {
                $composite = $tripod->getComposite($op);
                $modifiedSubjects = array_merge($modifiedSubjects,$composite->getImpactedSubjects($subjectsAndPredicatesOfChange,$this->args['contextAlias']));
            }

            if(!empty($modifiedSubjects)){
                /* @var $subject \Tripod\Mongo\ImpactedSubject */
                foreach ($modifiedSubjects as $subject) {
                    $subject->update();
                }
            }

            // stat time taken to process item, from time it was created (queued)
            $timer->stop();
            $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUCCESS,$timer->result());
            $this->debugLog("DiscoverImpactedSubjects::perform() done in {$timer->result()}ms");

        }
        catch(\Exception $e)
        {
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * @param array $data
     * @param string|null $queueName
     */
    public function createJob(Array $data, $queueName=null)
    {
        if(!$queueName)
        {
            $queueName = Config::getDiscoverQueueName();
        }
        $this->submitJob($queueName,get_class($this),$data);
    }

    /**
     * Validate args for DiscoverImpactedSubjects
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array("tripodConfig","storeName","podName","changes","operations","contextAlias");
    }

    /**
     * @param \Tripod\Mongo\ImpactedSubject $subject
     * @param string $queueName
     */
    protected function addSubjectToQueue(\Tripod\Mongo\ImpactedSubject $subject, $queueName)
    {
        $resourceId = $subject->getResourceId();
        $this->debugLog("Adding operation {$subject->getOperation()} for subject {$resourceId[_ID_RESOURCE]} to queue ".$queueName);
        $this->getApplyOperation()->createJob($subject, $queueName);
    }

    /**
     * For mocking
     * @return ApplyOperation
     */
    protected function getApplyOperation()
    {
        if(!isset($this->applyOperation))
        {
            $this->applyOperation = new ApplyOperation();
        }
        return $this->applyOperation;
    }


}
