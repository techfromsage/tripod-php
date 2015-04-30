<?php

class DiscoverImpactedSubjects extends JobBase {

    public function perform()
    {
        try
        {

            $this->debugLog("DiscoverImpactedSubjects::perform() start");

            $timer = new Timer();
            $timer->start();

            $this->validateArgs();

            // set the config to what is received
            MongoTripodConfig::setConfig($this->args["tripodConfig"]);

            $tripod = $this->getMongoTripod($this->args["storeName"],$this->args["podName"]);

            $operations = $this->args['operations'];
            $modifiedSubjects = array();

            // de-serialize changeset
            $cs = new ChangeSet();
            $cs->from_json($this->args["changeSet"]);

            foreach($operations as $op)
            {
                $composite = $tripod->getComposite($op);
                $modifiedSubjects = array_merge($modifiedSubjects,$composite->getImpactedSubjects($cs,$this->args['contextAlias']));
            }

            if(!empty($modifiedSubjects)){
                /* @var $subject ImpactedSubject */
                foreach ($modifiedSubjects as $subject) {
                    $resourceId = $subject->getResourceId();
                    $this->debugLog("Adding operation {$subject->getOperation()} for subject {$resourceId[_ID_RESOURCE]} to queue ".MongoTripodConfig::getApplyQueueName());
                    Resque::enqueue(MongoTripodConfig::getApplyQueueName(),"ApplyOperation",array(
                        "subject"=>$subject->toArray(),
                        "tripodConfig"=>$this->args["tripodConfig"]
                    ));
                }
            }

            // stat time taken to process item, from time it was created (queued)
            $timer->stop();
            $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUCCESS,$timer->result());
            $this->debugLog("DiscoverImpactedSubjects::perform() done in {$timer->result()}ms");

        }
        catch(Exception $e)
        {
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * Validate args for DiscoverImpactedSubjects
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array("tripodConfig","storeName","podName","changeSet","operations","contextAlias");
    }
}
