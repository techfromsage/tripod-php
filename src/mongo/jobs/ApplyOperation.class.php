<?php

class ApplyOperation extends JobBase {
    public function perform()
    {
        try
        {
            $this->debugLog("ApplyOperation::perform() start");

            $timer = new Timer();
            $timer->start();

            $this->validateArgs();

            // set the config to what is received
            MongoTripodConfig::setConfig($this->args["tripodConfig"]);

            $subjectAsArray = $this->args["subject"];
            $subject = new ImpactedSubject(
                $subjectAsArray["resourceId"],
                $subjectAsArray["operation"],
                $subjectAsArray["storeName"],
                $subjectAsArray["podName"],
                $subjectAsArray["specTypes"],
                $subjectAsArray["delete"]
            );

            $subject->update();

            $timer->stop();
            // stat time taken to process item, from time it was created (queued)
            $this->getStat()->timer(MONGO_QUEUE_APPLY_OPERATION_SUCCESS,$timer->result());

            $this->debugLog("ApplyOperation::perform() done in {$timer->result()}ms");
        }
        catch (Exception $e)
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
        return array("tripodConfig","subject");
    }
}
