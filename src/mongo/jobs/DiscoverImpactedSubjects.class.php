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
     * @var array
     */
    protected $subjectsGroupedByQueue = array();

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
                    if(isset($this->args['queue']) || count($subject->getSpecTypes()) == 0)
                    {
                        $queueName = (isset($this->args['queue']) ? $this->args['queue'] : Config::getApplyQueueName());
                        $this->addSubjectToQueue($subject, $queueName);
                    }
                    else
                    {
                        $specsGroupedByQueue = array();
                        foreach($subject->getSpecTypes() as $specType)
                        {
                            $spec = null;
                            switch($subject->getOperation())
                            {
                                case OP_VIEWS:
                                    $spec = Config::getInstance()->getViewSpecification($this->args["storeName"], $specType);
                                    break;
                                case OP_TABLES:
                                    $spec = Config::getInstance()->getTableSpecification($this->args["storeName"], $specType);
                                    break;
                                case OP_SEARCH:
                                    $spec = Config::getInstance()->getSearchDocumentSpecification($this->args["storeName"], $specType);
                                    break;
                            }
                            if(!$spec || !isset($spec['queue']))
                            {
                                if(!$spec)
                                {
                                    $spec = array();
                                }
                                $spec['queue'] = Config::getApplyQueueName();
                            }
                            if(!isset($specsGroupedByQueue[$spec['queue']]))
                            {
                                $specsGroupedByQueue[$spec['queue']] = array();
                            }
                            $specsGroupedByQueue[$spec['queue']][] = $specType;
                        }

                        foreach($specsGroupedByQueue as $queueName=>$specs)
                        {
                            $queuedSubject = new \Tripod\Mongo\ImpactedSubject(
                                $subject->getResourceId(),
                                $subject->getOperation(),
                                $subject->getStoreName(),
                                $subject->getPodName(),
                                $specs
                            );

                            $this->addSubjectToQueue($queuedSubject, $queueName);
                        }
                    }
                }
                if(!empty($this->subjectsGroupedByQueue))
                {
                    foreach($this->subjectsGroupedByQueue as $queueName=>$subjects)
                    {
                        $this->getApplyOperation()->createJob($subjects, $queueName);
                    }
                    $this->subjectsGroupedByQueue = array();
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
        if(!array_key_exists($queueName, $this->subjectsGroupedByQueue))
        {
            $this->subjectsGroupedByQueue[$queueName] = array();
        }
        $this->subjectsGroupedByQueue[$queueName][] = $subject;
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
