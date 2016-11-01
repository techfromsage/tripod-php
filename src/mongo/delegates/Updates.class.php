<?php

namespace Tripod\Mongo;

use Tripod\Exceptions\Exception;
use Tripod\IEventHook;
use \MongoDB\Driver\ReadPreference;
use \MongoDB\Database;
use \MongoDB\Collection;
use \MongoDB\Operation\FindOneAndUpdate;
use \MongoDB\BSON\UTCDateTime;
use \MongoDB\BSON\ObjectId;

require_once TRIPOD_DIR . 'mongo/Config.class.php';

/**
 * Class Updates
 * @package Tripod\Mongo
 */
class Updates extends DriverBase {

    /**
     * $var TransactionLog
     */
    private $transactionLog = null;

    /**
     * @var Labeller
     */
    protected $labeller;

    /**
     * @var array The original read preference gets stored here
     *            when changing for a write.
     */
    private $originalCollectionReadPreference = '';

    /**
     * @var array The original read preference gets stored here
     * when changing for a write.
     */
    private $originalDbReadPreference = '';

    /**
     * @var Driver
     */
    protected $tripod;


    /**
     * @var Integer
     */
    private $retriesToGetLock;

    /**
     * @var array
     */
    private $async = null;

    /**
     * @var \MongoDB
     */
    protected $locksDb;

    /**
     * @var Collection
     */
    protected $locksCollection;

    /**
     * @var string
     */
    private $queueName;

    /**
     * @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects
     */
    protected $discoverImpactedSubjects;

    /**
     * @var array
     */
    private $saveChangesHooks = array();

    /**
     * @param Driver $tripod
     * @param array $opts
     */
    public function __construct(Driver $tripod,$opts=array())
    {
        $this->tripod = $tripod;
        $this->storeName = $tripod->getStoreName();
        $this->podName = $tripod->getPodName();
        $this->stat = $tripod->getStat();
        $this->labeller = new Labeller();
        $opts = array_merge(array(
                'defaultContext'=>null,
                OP_ASYNC=>array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true),
                'stat'=>null,
                'readPreference' => ReadPreference::RP_PRIMARY_PREFERRED,
                'retriesToGetLock' => 20)
            ,$opts);
        $this->readPreference = $opts['readPreference'];
        $this->config = $this->getConfigInstance();

        // default context
        $this->defaultContext = $opts['defaultContext'];

        //max retries to get lock
        $this->retriesToGetLock = $opts['retriesToGetLock'];

        // fill in and default any missing keys for $async array
        $async = $opts[OP_ASYNC];
        if (!array_key_exists(OP_VIEWS,$async))
        {
            $async[OP_VIEWS] = false;
        }
        if (!array_key_exists(OP_TABLES,$async))
        {
            $async[OP_TABLES] = true;
        }

        if (!array_key_exists(OP_SEARCH,$async))
        {
            $async[OP_SEARCH] = true;
        }

        // if there is no es configured then remove OP_SEARCH from async (no point putting these onto the queue) TRI-19
        if($this->config->getSearchDocumentSpecifications($this->storeName) == null) {
            unset($async[OP_SEARCH]);
        }

        // If a custom queue name was specified, store it
        if(array_key_exists(OP_QUEUE, $async))
        {
            $this->queueName = $async[OP_QUEUE];
            unset($async[OP_QUEUE]);
        }

        $this->async = $async;

        if (isset($opts['statsConfig']))
        {
            $this->statsConfig = $opts['statsConfig'];
        }
    }

    /**
     * Create and apply a changeset which is the delta between $oldGraph and $newGraph
     * @param \Tripod\ExtendedGraph $oldGraph
     * @param \Tripod\ExtendedGraph $newGraph
     * @param string|null $context
     * @param string|null $description
     * @throws \Exception
     * @return bool
     */
    public function saveChanges(
        \Tripod\ExtendedGraph $oldGraph,
        \Tripod\ExtendedGraph $newGraph,
        $context=null,
        $description=null)
    {
        $this->applyHooks($this::HOOK_FN_PRE,$this->saveChangesHooks,array(
            "pod"=>$this->getPodName(),
            "oldGraph"=>$oldGraph,
            "newGraph"=>$newGraph,
            "context"=>$context
        ));

        $this->setReadPreferenceToPrimary();
        try{
            $contextAlias = $this->getContextAlias($context);

            if (!Config::getInstance()->isPodWithinStore($this->getStoreName(),$this->getPodName()))
            {
                throw new \Tripod\Exceptions\Exception("database:collection " . $this->getStoreName() . ":" . $this->getPodName(). " is not referenced within config, so cannot be written to");
            }

            $this->validateGraphCardinality($newGraph);

            $oldIndex = $oldGraph->get_index();
            $newIndex = $newGraph->get_index();
            $args = array('before' => $oldIndex, 'after' => $newIndex, 'changeReason' => $description);
            $cs = new \Tripod\ChangeSet($args);

            $subjectsAndPredicatesOfChange = array();
            $transaction_id = null;
            if ($cs->has_changes())
            {
                // store the actual CBDs
                $result = $this->storeChanges($cs, $contextAlias);
                if (!isset($result['subjectsAndPredicatesOfChange'])|| !isset($result['transaction_id'])) {
                    $this->errorLog("Result of storeChanges malformed, should have transaction_id and subjectsAndPredicatesOfChange array keys",array("result"=>$result));
                    throw new Exception("Result of storeChanges malformed, should have transaction_id and subjectsAndPredicatesOfChange array keys");
                }
                extract($result); // will unpack into $subjectsAndPredicatesOfChange

                // Process any syncronous operations
                $this->processSyncOperations($subjectsAndPredicatesOfChange,$contextAlias);

                // Schedule calculation of any async activity
                $this->queueAsyncOperations($subjectsAndPredicatesOfChange,$contextAlias);
            }

            $this->applyHooks($this::HOOK_FN_SUCCESS,$this->saveChangesHooks,array(
                "pod"=>$this->getPodName(),
                "oldGraph"=>$oldGraph,
                "newGraph"=>$newGraph,
                "context"=>$context,
                "changeSet"=>$cs,
                "subjectsAndPredicatesOfChange"=>$subjectsAndPredicatesOfChange,
                "transaction_id"=>$transaction_id
            ));
        }
        catch(\Exception $e){
            // ensure we reset the original read preference in the event of an exception
            $this->resetOriginalReadPreference();
            $this->applyHooks($this::HOOK_FN_FAILURE,$this->saveChangesHooks,array(
                "pod"=>$this->getPodName(),
                "oldGraph"=>$oldGraph,
                "newGraph"=>$newGraph,
                "context"=>$context
            ));
            throw $e;
        }

        $this->resetOriginalReadPreference();

        return true;
    }

    /**
     * Change the read preferences to RP_PRIMARY
     * Used for a write operation
     * @todo When there is a getReadPreference function available in the PHP Mongo library (or the read
     * preference is readable) we should use that instead of the debugInfo function we're using here.
     */
    protected function setReadPreferenceToPrimary()
    {
        // Set db preference
        /** @var ReadPreference $dbReadPref */
        $dbReadPref = $this->getDatabase()->__debugInfo()['readPreference'];

        $dbPref = $dbReadPref->getMode();
        $dbTagsets = $dbReadPref->getTagsets();

        $this->originalDbReadPreference = $this->db->__debugInfo()['readPreference']->getMode();
        if ($dbPref !== ReadPreference::RP_PRIMARY) {
            $this->db = $this->db->withOptions(array('readPreference' => new ReadPreference(ReadPreference::RP_PRIMARY, $dbTagsets)));
        }

        /** @var ReadPreference $collReadPref */
        $collReadPref = $this->getCollection()->__debugInfo()['readPreference'];
        $collPref = $collReadPref->getMode();
        $collTagsets = $collReadPref->getTagsets();

        // Set collection preference
        $this->originalCollectionReadPreference = $collPref;
        if ($collPref !== ReadPreference::RP_PRIMARY) {
            $this->collection = $this->collection->withOptions(array('readPreference' => new ReadPreference(ReadPreference::RP_PRIMARY, $collTagsets)));
        }
    }


    /**
     * Reset the original read preference after changing with setReadPreferenceToPrimary
     */
    protected function resetOriginalReadPreference(){

        /** @var ReadPreference $dbReadPref */
        $dbReadPref = $this->db->__debugInfo()['readPreference'];
        if($this->originalDbReadPreference !== $dbReadPref->getMode())
        {
            $pref = (isset($this->originalDbReadPreference)
                ? $this->originalDbReadPreference
                : $this->readPreference
            );
            $dbTagsets = $dbReadPref->getTagsets();

            $this->db = $this->db->withOptions(array(
                'readPreference' => new ReadPreference($pref, $dbTagsets)
            ));
        }

        // Reset collection object
        /** @var ReadPreference $collReadPref */
        $collReadPref = $this->getCollection()->__debugInfo()['readPreference'];
        if($this->originalCollectionReadPreference !== $collReadPref->getMode()){
            $pref = (isset($this->originalCollectionReadPreference)
                ? $this->originalCollectionReadPreference
                : $this->readPreference
            );
            $collTagsets = $collReadPref->getTagsets();
            $this->collection = $this->collection->withOptions(array(
                'readPreference' => new ReadPreference($pref, $collTagsets)
            ));
        }
    }

    /**
     * Ensure that the graph we want to persist has data with valid cardinality.
     *
     * @param \Tripod\ExtendedGraph $graph
     * @throws \Tripod\Exceptions\CardinalityException
     */
    protected function validateGraphCardinality(\Tripod\ExtendedGraph $graph)
    {
        $config = Config::getInstance();
        $cardinality = $config->getCardinality($this->getStoreName(), $this->getPodName());
        $namespaces = $config->getNamespaces();
        $graphSubjects = $graph->get_subjects();

        if (empty($cardinality) || $graph->is_empty())
        {
            return;
        }

        foreach ($cardinality as $qname=>$cardinalityValue)
        {
            list($namespace, $predicateName) = explode(':', $qname);
            if (!array_key_exists($namespace, $namespaces))
            {
                //TODO This may be changed to a namespace exception at some point...
                throw new \Tripod\Exceptions\CardinalityException("Namespace '{$namespace}' not defined for qname: {$qname}");
            }

            // NB: The only constraint we currently support is a value of 1 to enforce one triple per subject/predicate.
            if ($cardinalityValue == 1)
            {
                foreach ($graphSubjects as $subjectUri)
                {
                    $predicateUri = $namespaces[$namespace].$predicateName;
                    $predicateValues = $graph->get_subject_property_values($subjectUri, $predicateUri);
                    if (count($predicateValues) > 1)
                    {
                        $v = array();
                        foreach ($predicateValues as $predicateValue)
                        {
                            $v[] = $predicateValue['value'];
                        }
                        throw new \Tripod\Exceptions\CardinalityException("Cardinality failed on {$subjectUri} for '{$qname}' - should only have 1 value and has: ".implode(', ', $v));
                    }
                }
            }
        }
    }


    /**
     * @param \Tripod\ChangeSet $cs Change-set to apply
     * @param string $contextAlias
     * @throws \Tripod\Exceptions\Exception
     * @return array An array of subjects and predicates that have been changed
     */
    protected function storeChanges(\Tripod\ChangeSet $cs, $contextAlias)
    {
        $t = new \Tripod\Timer();
        $t->start();

        $subjectsOfChange = $cs->get_subjects_of_change();
        $transaction_id = $this->generateTransactionId();

        // store the details of the transaction in the transaction log
        $mongoGraph = new MongoGraph();
        $mongoGraph->_index = $cs->_index;
        $csDoc = $mongoGraph->to_tripod_view_array("changes",$contextAlias); // todo - this changed to tripod view array, why is "changes" the docId?
        $originalCBDs=array();

        // apply the changes
        try
        {
            // 1. lock all documents
            // 2. create new transaction
            // 3. apply changes
            // 4. unlock all documents
            // 5. complete transaction

            $originalCBDs = $this->lockAllDocuments($subjectsOfChange, $transaction_id,$contextAlias);

            $this->getTransactionLog()->createNewTransaction($transaction_id, $csDoc['value'][_GRAPHS], $originalCBDs, $this->getStoreName(), $this->getPodName());

            if(empty($originalCBDs)) // didn't get lock on documents
            {
                $this->getTransactionLog()->failTransaction($transaction_id, new \Exception('Did not obtain locks on documents'));
                throw new \Exception('Did not obtain locks on documents');
            }

            $changes = $this->applyChangeSet($cs,$originalCBDs,$contextAlias, $transaction_id);

            $this->debugLog(MONGO_LOCK,
                array(
                    'description'=>'Driver::storeChanges - Unlocking documents, apply change-set completed',
                    'transaction_id'=>$transaction_id,
                )
            );

            $this->unlockAllDocuments($transaction_id);
            $this->getTransactionLog()->completeTransaction($transaction_id, $changes['newCBDs']);

            $t->stop();
            $this->timingLog(MONGO_WRITE, array('duration'=>$t->result(), 'subjectsOfChange'=>implode(", ",$subjectsOfChange)));
            $this->getStat()->timer(MONGO_WRITE.".{$this->getPodName()}",$t->result());

            return $changes;
        }
        catch(\Exception $e)
        {
            $this->getStat()->increment(MONGO_ROLLBACK);
            $this->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'Save Failed Rolling back transaction:' . $e->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'subjectsOfChange'=>implode(",",$subjectsOfChange),
                    'mongoDriverError' => $this->getLastDBError($this->getDatabase()),
                    'exceptionMessage' => $e->getMessage()
                )
            );
            $this->rollbackTransaction($transaction_id, $originalCBDs, $e);

            throw new \Tripod\Exceptions\Exception('Error storing changes: '.$e->getMessage()." >>>" . $e->getTraceAsString());
        }
    }

    /**
     * @param string $transaction_id id of the transaction
     * @param array $originalCBDs containing the original CBDS
     * @param \Exception $exception
     * @throws \Exception
     * @return bool
     */
    protected function rollbackTransaction($transaction_id, $originalCBDs, \Exception $exception)
    {
        // set transaction to cancelling
        $this->getTransactionLog()->cancelTransaction($transaction_id, $exception);

        if (!empty($originalCBDs)) {  // restore the original CBDs
            foreach ($originalCBDs as $g)
            {
                /** @var UpdateResult $result */
                $result = $this->updateCollection(array(_ID_KEY => $g[_ID_KEY]), $g, array('w' => 1));

                if(!$result->isAcknowledged())
                {
                    // Error log here
                    $this->errorLog(MONGO_ROLLBACK,
                        array(
                            'description' => 'Driver::rollbackTransaction - Error updating transaction',
                            'exception_message' => $exception->getMessage(),
                            'transaction_id' => $transaction_id,
                            'mongoDriverError' => $this->getLastDBError($this->getDatabase())
                        )
                    );
                    throw new \Exception("Failed to restore Original CBDS for transaction: {$transaction_id} stopped at ".$g[_ID_KEY]);
                }
            }
        }
        else
        {
            $this->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'Driver::rollbackTransaction - Unlocking documents',
                    'exception_message' => $exception->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'mongoDriverError' => $this->getLastDBError($this->getDatabase())
                )
            );
        }
        $this->unlockAllDocuments($transaction_id);

        // set transaction to failed
        $this->getTransactionLog()->failTransaction($transaction_id);
        return true;
    }

    /**
     * Returns a unique transaction ID
     * @return string
     */
    protected function generateTransactionId()
    {
        return 'transaction_' . $this->getUniqId();
    }

    /**
     * Returns a unique id: for mocking
     * @return string
     */
    protected function getUniqId()
    {
        return uniqid('', true);
    }


    /**
     * Adds/updates/deletes the graph in the database
     * @param \Tripod\ChangeSet $cs
     * @param array $originalCBDs
     * @param string $contextAlias
     * @param string $transaction_id
     * @return array
     * @throws \Exception
     */
    protected function applyChangeSet(\Tripod\ChangeSet $cs, $originalCBDs, $contextAlias, $transaction_id)
    {
        $subjectsAndPredicatesOfChange = array();
        if (in_array($this->getCollection()->getCollectionName(), $this->getConfigInstance()->getPods($this->getStoreName())))
        {
            // how many subjects of change?
            /** @noinspection PhpParamsInspection */
            $changes = $cs->get_subjects_of_type($this->labeller->qname_to_uri("cs:ChangeSet"));

            // gather together all the updates (we'll apply them later)....
            $updates = array();

            $newCBDs = array();

            foreach ($changes as $changeUri)
            {
                $subjectOfChange = $cs->get_first_resource($changeUri,$this->labeller->qname_to_uri("cs:subjectOfChange"));
                if(!array_key_exists($subjectOfChange, $subjectsAndPredicatesOfChange))
                {
                    $subjectsAndPredicatesOfChange[$subjectOfChange] = array();
                }

                $criteria = array(
                    _ID_KEY=>array(_ID_RESOURCE=>$this->labeller->uri_to_alias($subjectOfChange),_ID_CONTEXT=>$contextAlias)
                );

                // read before write, and to find array indexes and get document in memory

                $doc = $this->getDocumentForUpdate($subjectOfChange, $contextAlias, $originalCBDs);

                $upsert = false;
                if(empty($doc))
                {
                    $upsert = true;
                }

                // add the old vals to critera
                $changesGroupedByNsPredicate = $this->getAdditionsRemovalsGroupedByNsPredicate($cs,$changeUri);


                $mongoUpdateOperations = array();
                foreach ($changesGroupedByNsPredicate as $nsPredicate => $additionsRemovals)
                {
                    $predicateExists = isset($doc[$nsPredicate]);

                    $predicate = $this->labeller->qname_to_uri($nsPredicate);
                    if(!in_array($predicate, $subjectsAndPredicatesOfChange[$subjectOfChange]))
                    {
                        $subjectsAndPredicatesOfChange[$subjectOfChange][] = $predicate;
                    }

                    // set to existing object if exists
                    $valueObject = ($predicateExists) ? $doc[$nsPredicate] : array();
                    if (isset($valueObject[VALUE_URI]) || isset($valueObject[VALUE_LITERAL]))
                    {
                        // this is a single value object, convert to array of values for now
                        $valueObject = array($valueObject);
                    }

                    if (isset($additionsRemovals["additions"]))
                    {
                        foreach ($additionsRemovals["additions"] as $addition)
                        {
                            $valueObject[] = $addition;
                        }
                    }

                    // remove
                    if (isset($additionsRemovals["removals"]))
                    {
                        $elemsToRemove = array();
                        foreach ($additionsRemovals["removals"] as $removal)
                        {
                            $valueIndex = array_search($removal, $valueObject);
                            if($valueIndex === false)
                            {
                                $v = array_pop(array_values($removal));
                                $this->errorLog("Removal value {$subjectOfChange} {$predicate} {$v} does not appear in target document to be updated",array("doc"=>$doc));
                                throw new \Exception("Removal value {$subjectOfChange} {$predicate} {$v} does not appear in target document to be updated");
                            }
                            else
                            {
                                $elemsToRemove[] = $valueIndex;
                            }
                        }
                        if (count($elemsToRemove)>0)
                        {
                            foreach($elemsToRemove as $elem)
                            {
                                unset($valueObject[$elem]);
                            }
                            $valueObject = array_values($valueObject); // renumbers array after unsets
                        }
                    }

                    if (count($valueObject)>0)
                    {
                        // unique value object
                        $valueObject = array_map("unserialize", array_unique(array_map("serialize", $valueObject)));

                        if (count($valueObject)==1)
                        {
                            $valueObject = $valueObject[0]; // un-array if only one value
                        }
                        $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_SET,array($nsPredicate=>$valueObject));
                    }
                    else
                    {
                        // remove all existing values, if existed in the first place
                        if ($predicateExists)
                        {
                            $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_UNSET,array($nsPredicate=>1));
                        }
                    }
                }

                //todo: criteria at this point should probably include all removal statements if they exist
                // i.e. we only want to update document if it has all these values ( think platform 409 )
                // currently the only criteria is the doc id
                //var_dump($targetGraph->to_tripod_array($subjectOfChange));

                $updatedAt = new UTCDateTime(floor(microtime(true))*1000);

                if (!isset($doc[_VERSION]))
                {
                    // new doc
                    $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_SET,array(_VERSION=>0));
                    $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_SET,array(_CREATED_TS=>$updatedAt));
                }
                else
                {
                    $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_INC,array(_VERSION=>1));
                }

                $this->addOperatorToChange($mongoUpdateOperations,MONGO_OPERATION_SET,array(_UPDATED_TS=>$updatedAt));

                $updates[] = array("criteria"=>$criteria,"changes"=>$mongoUpdateOperations,"upsert"=>$upsert);
            }

            // apply each update
            foreach ($updates as $update)
            {
                try
                {
                    $newDoc = $this->getCollection()->findOneAndUpdate($update['criteria'], $update['changes'], array(
                        'upsert' => $update['upsert'],
                        'returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER
                    ));
                    array_push($newCBDs, $newDoc);
                }
                catch (\Exception $e)
                {
                    $this->errorLog(MONGO_WRITE,
                        array(
                            'description'=>'Error with Mongo DB findOneAndUpdate:' . $e->getMessage(),
                            'transaction_id'=>$transaction_id,
                            'mongoDriverError' => $this->getLastDBError($this->getDatabase())
                        )
                    );
                    throw new \Exception($e);
                }
            }

            return array(
                'newCBDs'=>$newCBDs,
                'subjectsAndPredicatesOfChange'=>$this->subjectsAndPredicatesOfChangeUrisToAliases($subjectsAndPredicatesOfChange),
                'transaction_id'=>$transaction_id
            );
        }
        else
        {
            throw new \Exception("Attempted to update a non-CBD collection");
        }
    }

    /**
     * Helper function to group the changes for $changeUri by namespaced predicate, then by additions and removals
     * @param \Tripod\ChangeSet $cs
     * @param $changeUri
     * @return array
     */
    private function getAdditionsRemovalsGroupedByNsPredicate(\Tripod\ChangeSet $cs, $changeUri)
    {
        $additionsGroupedByNsPredicate = $this->getChangesGroupedByNsPredicate($cs,$changeUri,$this->labeller->qname_to_uri("cs:addition"));
        $removalsGroupedByNsPredicate = $this->getChangesGroupedByNsPredicate($cs,$changeUri,$this->labeller->qname_to_uri("cs:removal"));

        $mergedResult = array();
        foreach($additionsGroupedByNsPredicate as $predicate => $values)
        {
            if (!isset($mergedResult[$predicate]))
            {
                $mergedResult[$predicate] = array();
            }
            $mergedResult[$predicate]["additions"] = $values;
        }
        foreach($removalsGroupedByNsPredicate as $predicate => $values)
        {
            if (!isset($mergedResult[$predicate]))
            {
                $mergedResult[$predicate] = array();
            }
            $mergedResult[$predicate]["removals"] = $values;
        }
        return $mergedResult;
    }

    /**
     * Helper method to group changes for $changeUri of a given type by namespaced predicate
     * @param \Tripod\ChangeSet $cs
     * @param array $changes
     * @return array
     * @throws \Tripod\Exceptions\Exception
     */
    private function getChangesGroupedByNsPredicate(\Tripod\ChangeSet $cs, $changeUri, $changePredicate)
    {
        $changes = $cs->get_subject_property_values($changeUri,$changePredicate);

        $changesGroupedByNsPredicate = array();
        foreach ($changes as $c)
        {
            $predicate = $cs->get_first_resource($c["value"],$this->labeller->qname_to_uri("rdf:predicate"));
            $nsPredicate = $this->labeller->uri_to_qname($predicate);

            if(!array_key_exists($nsPredicate, $changesGroupedByNsPredicate))
            {
                $changesGroupedByNsPredicate[$nsPredicate] = array();
            }

            $object = $cs->get_subject_property_values($c["value"],$this->labeller->qname_to_uri("rdf:object"));
            if (count($object)!=1)
            {
                $this->getLogger()->error("Expecting object array with exactly 1 element",$object);
                throw new \Tripod\Exceptions\Exception("Object of removal malformed");
            }

            $valueType = (($object[0]['type']=="uri")) ? VALUE_URI : VALUE_LITERAL;
            $value = ($valueType===VALUE_URI) ? $this->labeller->uri_to_alias($object[0]["value"]) : $object[0]["value"];

            $changesGroupedByNsPredicate[$nsPredicate][] = array($valueType=>$value);
        }
        return $changesGroupedByNsPredicate;
    }

    /**
     * Helper method to add operator to a set of existing changes ready to be sent to Mongo
     * @param $changes
     * @param $operator
     * @param $kvp
     */
    private function addOperatorToChange(&$changes,$operator,$kvp)
    {
        if (!isset($changes[$operator]) || !is_array($changes[$operator]))
        {
            $changes[$operator] = array();
        }
        foreach($kvp as $key=>$value)
        {
            if (isset($changes[$operator][$key]))
            {
                $value = array_merge($value,$changes[$operator][$key]);
            }
            $changes[$operator][$key] = $value;
        }
    }

    /**
     * Normalize our subjects and predicates of change to use aliases rather than fq uris
     * @param array $subjectsAndPredicatesOfChange
     * @return array
     */
    protected function subjectsAndPredicatesOfChangeUrisToAliases(array $subjectsAndPredicatesOfChange)
    {
        $aliases = array();
        foreach($subjectsAndPredicatesOfChange as $subject=>$predicates)
        {
            $subjectAlias = $this->labeller->uri_to_alias($subject);
            $aliases[$subjectAlias] = array();
            foreach($predicates as $predicate)
            {
                $aliases[$subjectAlias][] = $this->labeller->uri_to_alias($predicate);
            }
        }
        return $aliases;
    }

    /**
     * Given a set of CBD's return the CBD that matches the Subject of Change
     * @param string $subjectOfChange
     * @param string $contextAlias
     * @param array $cbds
     * @return null | array the document from the collection of $cbds that matches the subject of change
     */
    protected function getDocumentForUpdate($subjectOfChange, $contextAlias, Array $cbds)
    {
        foreach($cbds as $c)
        {
            if($c[_ID_KEY]==array(_ID_RESOURCE=>$this->labeller->uri_to_alias($subjectOfChange),_ID_CONTEXT=>$contextAlias))
            {
                return $c;
                break;
            }
        }

        return null;
    }

    /**
     * Processes each subject synchronously
     * @param array $subjectsAndPredicatesOfChange
     * @param string $contextAlias
     */
    protected function processSyncOperations(Array $subjectsAndPredicatesOfChange, $contextAlias)
    {
        foreach($this->getSyncOperations() as $op)
        {
            /** @var \Tripod\Mongo\Composites\IComposite $composite */
            $composite = $this->tripod->getComposite($op);
            $opSubjects = $composite->getImpactedSubjects($subjectsAndPredicatesOfChange,$contextAlias);
            if (!empty($opSubjects)) {
                foreach($opSubjects as $subject)
                {
                    /* @var $subject ImpactedSubject */
                    $t = new \Tripod\Timer();
                    $t->start();

                    // Call update on the subject, rather than the composite directly, in case the change was to
                    // another pod
                    $subject->update($subject);

                    $t->stop();

                    $this->timingLog(MONGO_ON_THE_FLY_MR,array(
                        "duration"=>$t->result(),
                        "storeName"=>$subject->getStoreName(),
                        "podName"=>$subject->getPodName(),
                        "resourceId"=>$subject->getResourceId()
                    ));
                    $this->getStat()->timer(MONGO_ON_THE_FLY_MR,$t->result());
                }
            }
        }

    }


    /////////////////////////// QUEUE RELATED METHODS BELOW HERE ///////////////////////////////////////


    /**
     * Adds the operations to the queue to be performed asynchronously
     * @param array $subjectsAndPredicatesOfChange
     * @param string $contextAlias
     */
    protected function queueASyncOperations(Array $subjectsAndPredicatesOfChange,$contextAlias)
    {
        $operations = $this->getAsyncOperations();
        if (!empty($operations)) {
            $data = array(
                "changes" => $subjectsAndPredicatesOfChange,
                "operations" => $operations,
                "tripodConfig" => Config::getConfig(),
                "storeName" => $this->storeName,
                "podName" => $this->podName,
                "contextAlias" => $contextAlias,
                "statsConfig"=>$this->getStatsConfig()
            );


            if(isset($this->queueName))
            {
                $data[OP_QUEUE] = $this->queueName;
                $queueName = $this->queueName;
            }
            else
            {
                $queueName =  Config::getDiscoverQueueName();
            }

            $this->getDiscoverImpactedSubjects()->createJob($data, $queueName);
        }
    }

    /**
     * For mocking
     * @return Jobs\DiscoverImpactedSubjects
     */
    protected function getDiscoverImpactedSubjects()
    {
        if(!isset($this->discoverImpactedSubjects))
        {
            $this->discoverImpactedSubjects = new \Tripod\Mongo\Jobs\DiscoverImpactedSubjects();
        }
        return $this->discoverImpactedSubjects;
    }

    //////// LOCKS \\\\\\\\

    /**
     * Get locked documents for a date range or all documents if no date range is given
     * @param string $fromDateTime
     * @param string $tillDateTime
     * @return array
     */
    public function getLockedDocuments($fromDateTime = null , $tillDateTime = null)
    {
        $query = array();
        if(!empty($fromDateTime) || !empty($tillDateTime)){
            $query[_LOCKED_FOR_TRANS_TS] = array();

            if (!empty($fromDateTime)) {
                $query[_LOCKED_FOR_TRANS_TS][MONGO_OPERATION_GTE] = new UTCDateTime(strtotime($fromDateTime)*1000);
            }
            if (!empty($tillDateTime)) {
                $query[_LOCKED_FOR_TRANS_TS][MONGO_OPERATION_LTE] = new UTCDateTime(strtotime($tillDateTime)*1000);
            }
        }
        $docs = $this->getLocksCollection()->find($query, array('sort' => array(_LOCKED_FOR_TRANS => 1)));

        if($this->getLocksCollection()->count($query) == 0 ) {
            return array();
        }

        $res = array();
        foreach($docs as $doc){
            $res[] = $doc;
        }
        return $res;
    }

    /**
     * Attempts to lock all subjects of change in a pass, if failed unlocked locked subjects and do a retry of all again.
     * @param array $subjectsOfChange array of the subjects that are part of this transaction
     * @param string $transaction_id id for this transaction
     * @param string $contextAlias
     * @return array|null returns an array of CBDs, each CBD is the version at the time at which the lock was attained
     * @throws \Exception
     */
    protected function lockAllDocuments($subjectsOfChange, $transaction_id, $contextAlias)
    {
        for($retry=1; $retry <= $this->retriesToGetLock; $retry++)
        {
            $originalCBDs = array();
            $lockedSubjects = array();
            foreach ($subjectsOfChange as $s)
            {
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'Driver::lockAllDocuments - Attempting to get lock',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'attempt' => $retry
                    )
                );

                $document = $this->lockSingleDocument($s, $transaction_id, $contextAlias);
                if(!empty($document)){

                    $this->debugLog(MONGO_LOCK,
                        array(
                            'description'=>'Driver::lockAllDocuments - Got the lock',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'retry' => $retry
                        )
                    );

                    $this->getStat()->increment(MONGO_LOCK);
                    $originalCBDs[] = $document;
                    $lockedSubjects[] = $s;
                }
            }

            if(count($subjectsOfChange) == count($lockedSubjects))
            {
                //if all subjects of change locked, we are good.
                return $originalCBDs;
            }
            else
            {
                // If any subject was locked, unlock it
                if(count($lockedSubjects))
                {
                    $this->unlockAllDocuments($transaction_id);
                }
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>"Driver::lockAllDocuments - Unable to lock all ". count($subjectsOfChange) ."  documents, unlocked  " . count($lockedSubjects) . " locked documents",
                        'transaction_id'=>$transaction_id,
                        'documentsToLock' => implode(",", $subjectsOfChange),
                        'documentsLocked' => implode(",", $lockedSubjects),
                        'retry' => $retry
                    )
                );
                $n = mt_rand (25,40); usleep($n*1000); //do a retry
            }
        }

        $this->errorLog(MONGO_LOCK,
            array(
                'description'=>'Unable to lock all required documents. Exhausted retries',
                'retries' => $this->retriesToGetLock,
                'transaction_id'=>$transaction_id,
                'subjectsOfChange'=>implode(", ",$subjectsOfChange),
                'mongoDriverError' => $this->getLastDBError($this->getDatabase()),
            )
        );
        return NULL;
    }

    /**
     * Remove locks that are there forever, creates a audit entry to keep track who and why removed these locks.
     * @param string $transaction_id
     * @param string $reason
     * @return bool
     * @throws \Exception, if something goes wrong when unlocking documents, or creating audit entries.
     */
    public function removeInertLocks($transaction_id, $reason)
    {
        $query = array(_LOCKED_FOR_TRANS => $transaction_id);
        $docs = $this->getLocksCollection()->find($query);

        if($this->getLocksCollection()->count($query) == 0 ) {
            return false;
        }else{

            //1. Create audit entry with in_progress status
            $auditCollection  = $this->getAuditManualRollbacksCollection();
            $auditDocumentId = $this->generateIdForNewMongoDocument();
            try{
                $documents = array();
                foreach($docs as $doc)
                {
                    $documents[] = $doc[_ID_KEY][_ID_RESOURCE];
                }

                $result = $auditCollection->insertOne(
                    array(
                        _ID_KEY => $auditDocumentId,
                        'type' => AUDIT_TYPE_REMOVE_INERT_LOCKS,
                        'status' => AUDIT_STATUS_IN_PROGRESS,
                        'reason' => $reason,
                        'transaction_id' => $transaction_id,
                        'documents' => $documents,
                        _CREATED_TS=> $this->getMongoDate(),
                    )
                );
                if (!$result->isAcknowledged()) {
                    $error = $this->getLastDBError($this->getDatabase());
                    throw new \Exception("Failed to create audit entry with error message- " . $error['err']);
                }
            }
            catch(\Exception $e) { //simply send false as status as we are unable to create audit entry
                $this->errorLog(MONGO_LOCK,
                    array(
                        'description'=>'Driver::removeInertLocks - failed',
                        'transaction_id'=>$transaction_id,
                        'exception-message' => $e->getMessage()
                    )
                );
                throw $e;
            }

            //we can not try to combine this try-catch with try-catch above.
            //Catch below is supposed to update audit entry with error status but in above catch error can occur when creating audit entry.
            try{
                //2. Unlock documents linked to transaction
                $this->unlockAllDocuments($transaction_id);

                //3. Update audit entry to say it was completed

                $result = $auditCollection->updateOne(array(_ID_KEY => $auditDocumentId), array(MONGO_OPERATION_SET => array("status" => AUDIT_STATUS_COMPLETED, _UPDATED_TS => $this->getMongoDate())));

                if(!$result->isAcknowledged())
                {
                    $error = $this->getLastDBError($this->getDatabase());
                    throw new \Exception("Failed to update audit entry with error message- " . $error['err']);
                }
            }
            catch(\Exception $e) {
                $logInfo = array(
                    'description'=>'Driver::removeInertLocks - failed',
                    'transaction_id'=>$transaction_id,
                    'exception-message' => $e->getMessage()
                );

                //4. Update audit entry to say it was failed with error
                $result = $auditCollection->updateOne(array(_ID_KEY => $auditDocumentId), array(MONGO_OPERATION_SET => array("status" => AUDIT_STATUS_ERROR, _UPDATED_TS => $this->getMongoDate(), 'error' => $e->getMessage())));

                if(!$result->isAcknowledged())
                {
                    $error = $this->getLastDBError($this->getDatabase());
                    $logInfo['additional-error']=  "Failed to update audit entry with error message- " . $error['err'];
                }

                $this->errorLog(MONGO_LOCK, $logInfo);
                throw $e;
            }
        }
        return true;
    }

    /**
     * Unlocks documents locked by current transaction
     * @param string $transaction_id id for this transaction
     * @return bool
     * @throws \Exception is thrown if for any reason the update to mongo fails
     */
    protected function unlockAllDocuments($transaction_id)
    {
        $result = $this->getLocksCollection()->deleteMany(array(_LOCKED_FOR_TRANS => $transaction_id), array('w' => 1));

        if(!$result->isAcknowledged()) {
            $this->errorLog(MONGO_LOCK,
                array(
                    'description'=>'Driver::unlockAllDocuments - Failed to unlock documents (transaction_id - ' .$transaction_id .')',
                    'mongoDriverError' => $this->getLastDBError($this->getLocksDatabase()),
                    $result
                )
            );
            throw new \Exception("Failed to unlock documents as part of transaction : ".$transaction_id);
        }
        return true;
    }


    /**
     * Lock and return a single document for editing
     *
     * @param string $s subject URI of resource to lock
     * @param string $transaction_id
     * @param string $contextAlias
     * @return array
     */
    protected function lockSingleDocument($s, $transaction_id, $contextAlias)
    {
        $countEntriesInLocksCollection = $this->getLocksCollection()
            ->count(
                array(
                    _ID_KEY => array(
                        _ID_RESOURCE => $this->labeller->uri_to_alias($s),
                        _ID_CONTEXT => $contextAlias)
                )
            );

        if ($countEntriesInLocksCollection > 0){ //Subject is already locked
            return false;
        } else {
            try{ //Add a entry to locks collection for this subject, will throws exception if an entry already there
                $result = $this->getLocksCollection()->insertOne(
                    array(
                        _ID_KEY => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias),
                        _LOCKED_FOR_TRANS => $transaction_id,
                        _LOCKED_FOR_TRANS_TS => new UTCDateTime(floor(microtime(true))*1000)
                    ),
                    array("w" => 1)
                );

                if (!$result->isAcknowledged()) {
                    $error = $this->getLastDBError($this->getDatabase());
                    throw new \Exception("Failed to lock document with error message- " . $error['err']);
                }
            }
            catch(\Exception $e) { //Subject is already locked or unable to lock
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'Driver::lockSingleDocument - failed with exception',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'exception-message' => $e->getMessage()
                    )
                );
                return false;
            }

            //Let's get original document for processing.
            $document  = $this->getCollection()->findOne(array(_ID_KEY => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
            if(empty($document)){ //if document is not there, create it
                try{
                    $result = $this->getCollection()->insertOne(
                        array(
                            _ID_KEY => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)
                        ),
                        array("w" => 1)
                    );

                    if (!$result->isAcknowledged()) {
                        $error = $this->getLastDBError($this->getDatabase());
                        throw new \Exception("Failed to create new document with error message- " . $error['err']);
                    }
                    $document  = $this->getCollection()->findOne(array(_ID_KEY => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
                }
                catch(\Exception $e){
                    $this->errorLog(MONGO_LOCK,
                        array(
                            'description'=>'Driver::lockSingleDocument - failed when creating new document',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'exception-message' => $e->getMessage(),
                            'mongoDriverError' => $this->getDatabase()->lastError()
                        )
                    );
                    return false;
                }
            }
            return $document;
        }
    }

    /// Collection methods

    /**
     * @return Collection
     */
    protected function getAuditManualRollbacksCollection()
    {
        return $this->config->getCollectionForManualRollbackAudit($this->storeName);
    }

    /**
     * For mocking
     * @return Config
     */
    protected function getConfigInstance()
    {
        return Config::getInstance();
    }

    /**
     * @return ObjectId
     */
    protected function generateIdForNewMongoDocument()
    {
        return new ObjectId();
    }

    /**
     * @return UTCDateTime
     */
    protected function getMongoDate()
    {
        return new UTCDateTime(floor(microtime(true))*1000);
    }


    ///////// REPLAY TRANSACTION LOG ///////

    /**
     * replays all transactions from the transaction log, use the function params to control the from and to date if you
     * only want to replay transactions created during specific window
     * @param null $fromDate
     * @param null $toDate
     * @return bool
     */
    public function replayTransactionLog($fromDate=null, $toDate=null)
    {

        $cursor = $this->getTransactionLog()->getCompletedTransactions($this->storeName, $this->podName, $fromDate, $toDate);
        foreach($cursor as $result)
        {
            $this->applyTransaction($result);
        }

        return true;
    }

    // getters and setters for the delegates

    /**
     * @return TransactionLog
     */
    public function getTransactionLog()
    {
        if($this->transactionLog==null)
        {
            $this->transactionLog = new TransactionLog();
        }
        return $this->transactionLog;
    }

    /**
     * @param TransactionLog $transactionLog
     */
    public function setTransactionLog(TransactionLog $transactionLog)
    {
        $this->transactionLog = $transactionLog;
    }

    /**
     * Register save changes event hooks
     * @param IEventHook $hook
     */
    public function registerSaveChangesEventHook(IEventHook $hook)
    {
        $this->saveChangesHooks[] = $hook;
    }

    /**
     * Saves a transaction
     * @param array $transaction
     */
    protected function applyTransaction(Array $transaction)
    {
        $changes = $transaction['changes'];
        $newCBDs = $transaction['newCBDs'];

        $subjectsOfChange = array();
        foreach($changes as $c)
        {
            if($c['rdf:type'][VALUE_URI]=="cs:ChangeSet")
            {
                array_push($subjectsOfChange, $c['cs:subjectOfChange']['u']);
            }
        }

        foreach($subjectsOfChange as $s)
        {
            foreach($newCBDs as $n)
            {
                if($n[_ID_KEY][_ID_RESOURCE]==$s)
                {
                    $this->updateCollection(array(_ID_KEY=>$n[_ID_KEY]), $n, array('upsert'=>true));
                    break;
                }
            }
        }
    }

    /**
     * Creates a new Driver instance
     * @param array $data
     * @return Driver
     */
    protected function getTripod($data) {
        return new Driver(
            $data['collection'],
            $data['database'],
            array('stat'=>$this->stat));
    }

    /**
     * This proxy method allows us to mock updates against $this->collection
     * @param $query
     * @param $update
     * @param $options
     * @return bool
     */
    protected function updateCollection($query, $update, $options)
    {
        return $this->getCollection()->replaceOne($query, $update, $options);
    }

    /**
     * Returns the context alias curie for the supplied context or default context
     * @param string|null $context
     * @return string
     */
    protected function getContextAlias($context=null)
    {
        $contextAlias = $this->labeller->uri_to_alias((empty($context)) ? $this->defaultContext : $context);
        return (empty($contextAlias)) ? Config::getInstance()->getDefaultContextAlias() : $contextAlias;
    }

    /**
     * @return Database
     */
    protected function getLocksDatabase()
    {
        if(!isset($this->locksDb))
        {
            $this->locksDb = $this->config->getDatabase($this->storeName);
        }
        return $this->locksDb;
    }

    /**
     * @return Collection
     */
    protected function getLocksCollection()
    {
        if(!isset($this->locksCollection))
        {
            $this->locksCollection = $this->getLocksDatabase()->selectCollection(LOCKS_COLLECTION);
        }
        return $this->locksCollection;

    }

    /**
     * @return array
     */
    private function getAsyncOperations()
    {
        $types = array();
        foreach ($this->async as $op=>$isAsync) {
            if ($isAsync)
            {
                $types[] = $op;
            }
        }
        return $types;
    }

    /**
     * @return array
     */
    private function getSyncOperations()
    {
        $types = array();
        foreach ($this->async as $op=>$isAsync) {
            if (!$isAsync)
            {
                $types[] = $op;
            }
        }
        return $types;
    }
}