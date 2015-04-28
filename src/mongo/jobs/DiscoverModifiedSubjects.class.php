<?php

class DiscoverModifiedSubjects extends JobBase {

    public function perform()
    {
        try
        {

            $this->debugLog("DiscoverModifiedSubjects::perform() start");

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
                $modifiedSubjects = array_merge($modifiedSubjects,$composite->getModifiedSubjects($cs,$this->args['deletedSubjects'],$this->args['contextAlias']));
            }

            if(!empty($modifiedSubjects)){
                /* @var $subject ModifiedSubject */
                foreach ($modifiedSubjects as $subject) {
                    $subjectData = $subject->getData();
                    $this->debugLog("Adding operation {$subject->getOperation()} for subject {$subjectData["r"]} to queue ".TRIPOD_APPLY_QUEUE);
                    Resque::enqueue(TRIPOD_APPLY_QUEUE,"ApplyOperation",array(
                        "operation"=>$subject->getOperation(),
                        "subjectData"=>$subjectData,
                        "tripodConfig"=>$this->args["tripodConfig"]
                    ));
                }
            }

            // stat time taken to process item, from time it was created (queued)
            $timer->stop();
            $this->getStat()->timer(MONGO_QUEUE_DISCOVER_SUCCESS,$timer->result());
            $this->debugLog("DiscoverModifiedSubjects::perform() done in {$timer->result()}ms");

        }
        catch(Exception $e)
        {
            $this->errorLog("Caught exception in ".get_class($this).": ".$e->getMessage());
            throw $e;
        }
    }

    /**
     * Validate args for DiscoverModifiedSubjects
     * @return array
     */
    protected function getMandatoryArgs()
    {
        return array("tripodConfig","storeName","podName","changeSet","operations","deletedSubjects","contextAlias");
    }
}
