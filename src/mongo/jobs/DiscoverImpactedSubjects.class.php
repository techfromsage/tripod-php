<?php

namespace Tripod\Mongo\Jobs;

use \Tripod\Mongo\Config;
/**
 * Class DiscoverImpactedSubjects
 * @package Tripod\Mongo\Jobs
 */
class DiscoverImpactedSubjects extends JobBase {

    const STORE_NAME_KEY = 'storeName';
    const POD_NAME_KEY = 'podName';
    const OPERATIONS_KEY = 'operations';
    const CHANGES_KEY = 'changes';
    const CONTEXT_ALIAS_KEY = 'contextAlias';

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
            \Tripod\Mongo\Config::setConfig($this->args[self::TRIPOD_CONFIG_KEY]);

            $statsConfig = array();
            if(isset($this->args['statsConfig']))
            {
                $statsConfig['statsConfig'] = $this->args['statsConfig'];
            }
            $tripod = $this->getTripod($this->args[self::STORE_NAME_KEY],$this->args[self::POD_NAME_KEY],$statsConfig);

            $operations = $this->args[self::OPERATIONS_KEY];

            $subjectsAndPredicatesOfChange = $this->args[self::CHANGES_KEY];

            foreach($operations as $op)
            {
                /** @var \Tripod\Mongo\Composites\IComposite $composite */
                $composite = $tripod->getComposite($op);
                $modifiedSubjects = $composite->getImpactedSubjects($subjectsAndPredicatesOfChange,$this->args[self::CONTEXT_ALIAS_KEY]);
                if(!empty($modifiedSubjects)){
                    /* @var $subject \Tripod\Mongo\ImpactedSubject */
                    foreach ($modifiedSubjects as $subject) {
                        if(isset($this->args[self::QUEUE_KEY]) || count($subject->getSpecTypes()) == 0)
                        {
                            $queueName = (isset($this->args[self::QUEUE_KEY]) ? $this->args[self::QUEUE_KEY] : Config::getApplyQueueName());
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
                                        $spec = Config::getInstance()->getViewSpecification($this->args[self::STORE_NAME_KEY], $specType);
                                        break;
                                    case OP_TABLES:
                                        $spec = Config::getInstance()->getTableSpecification($this->args[self::STORE_NAME_KEY], $specType);
                                        break;
                                    case OP_SEARCH:
                                        $spec = Config::getInstance()->getSearchDocumentSpecification($this->args[self::STORE_NAME_KEY], $specType);
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
                            $this->getApplyOperation()->createJob($subjects, $queueName, $statsConfig);
                        }
                        $this->subjectsGroupedByQueue = array();
                    }
                }
            }

            // stat time taken to process item, from time it was created (queued)
            $timer->stop();
            $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUCCESS,$timer->result());
            $this->debugLog("DiscoverImpactedSubjects::perform() done in {$timer->result()}ms");

        }
        catch(\Exception $e)
        {
            $this->getStat()->increment(MONGO_QUEUE_DISCOVER_FAIL);
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
        elseif(strpos($queueName, \Tripod\Mongo\Config::getDiscoverQueueName()) === false)
        {
            $queueName = \Tripod\Mongo\Config::getDiscoverQueueName() . '::' . $queueName;
        }
        $this->submitJob($queueName,get_class($this),$data);
    }

    /**
     * Validate args for DiscoverImpactedSubjects
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array(
            self::TRIPOD_CONFIG_KEY,
            self::STORE_NAME_KEY,
            self::POD_NAME_KEY,
            self::CHANGES_KEY,
            self::OPERATIONS_KEY,
            self::CONTEXT_ALIAS_KEY
        );
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
