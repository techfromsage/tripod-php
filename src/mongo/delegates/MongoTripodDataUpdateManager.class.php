<?php

require_once TRIPOD_DIR . 'mongo/MongoTripodConfig.class.php';

class MongoTripodDataUpdateManager {
    protected $labeller;

    /**
     * @var array The original read preference gets stored here
     *            when changing for a write.
     */
    private $originalReadPreference = array();

    protected $tripod;


    /**
     * @var Integer
     */
    private $retriesToGetLock;

    /**
     * @var array
     */
    private $async = null;

    public function __construct(MongoTripod $tripod,$opts=array())
    {
        $this->tripod = $tripod;
        
        $this->labeller = new MongoTripodLabeller();
        $opts = array_merge(array(
                'defaultContext'=>null,
                OP_ASYNC=>array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true),
                'stat'=>null,
                'readPreference'=>MongoClient::RP_PRIMARY_PREFERRED,
                'retriesToGetLock' => 20)
            ,$opts);

        $this->config = $this->getMongoTripodConfigInstance();

        // default context
        $this->defaultContext = $opts['defaultContext'];

        //max retries to get lock
        $this->retriesToGetLock = $opts['retriesToGetLock'];

        //select locks collection
        $this->lCollection = $this->tripod->db->selectCollection(LOCKS_COLLECTION);

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
        if($this->config->searchDocSpecs == null) {
            unset($async[OP_SEARCH]);
        }

        $this->async = $async;

        // is a custom stat tracker passed in?
        if ($opts['stat']!=null) $this->stat = $opts['stat'];
    }
    public function saveChanges(
        ExtendedGraph $oldGraph,
        ExtendedGraph $newGraph,
        $context=null,
        $description=null)
    {
        $this->setReadPreferenceToPrimary();
        try{
            $contextAlias = $this->getContextAlias($context);

            if (!MongoTripodConfig::getInstance()->isCollectionWithinConfig($this->tripod->getDBName(),$this->tripod->getCollectionName()))
            {
                throw new TripodException("database:collection {$this->tripod->getDBName()}:{$this->tripod->getCollectionName()} is not referenced within config, so cannot be written to");
            }

            $this->validateGraphCardinality($newGraph);

            $oldIndex = $oldGraph->get_index();
            $newIndex = $newGraph->get_index();
            $args = array('before' => $oldIndex, 'after' => $newIndex, 'changeReason' => $description);
            $cs = new ChangeSet($args);

            if ($cs->has_changes())
            {
                $subjectsAndPredicatesOfChange = array();
                /** @noinspection PhpParamsInspection */
                $changes = $cs->get_subjects_of_type($oldGraph->qname_to_uri("cs:ChangeSet"));
                foreach ($changes as $change)
                {
                    $subject = $cs->get_first_resource($change,$oldGraph->qname_to_uri("cs:subjectOfChange"));

                    if(!isset($subjectsAndPredicatesOfChange[$subject]))
                    {
                        $subjectsAndPredicatesOfChange[$subject] = array();
                    }
                    // If resource is not either completely new or deleted, specify the predicates affected
                    if(!(empty($cs->before) || empty($cs->after)))
                    {
                        foreach($cs->get_subjects_where_resource(RDF_SUBJECT, $subject) as $changeNode)
                        {
                            foreach($cs->get_resource_triple_values($changeNode, RDF_PREDICATE) as $property)
                            {
                                $subjectsAndPredicatesOfChange[$subject][] = $this->labeller->uri_to_alias($property);
                            }
                        }
                    }
                }

                foreach($subjectsAndPredicatesOfChange as $subject=>$predicates)
                {
                    $subjectsAndPredicatesOfChange[$subject] = array_unique($predicates);
                }

                $changes = $this->storeChanges($cs, array_keys($subjectsAndPredicatesOfChange),$contextAlias);

                // calculate what operations need performing, based on the subjects and anything they impact
                $operationsToPerform  = $this->getApplicableOperations($subjectsAndPredicatesOfChange, $contextAlias, $this->async);
                $impactedOperations   = $this->getImpactedOperations($subjectsAndPredicatesOfChange, $contextAlias, $this->async);

                foreach($impactedOperations as $synckey=>$ops){
                    foreach($ops as $key=>$op){
                        if(!array_key_exists($key, $operationsToPerform[$synckey])){
                            $operationsToPerform[$synckey][$key] = $op;
                        }
                    }
                }

                // create subjects to process synchronously
                $syncModifiedSubjects = array();
                foreach($operationsToPerform[OP_SYNC] as $syncOp){
                    if(in_array($syncOp['id']['r'], $changes['deletedSubjects'])){
                        $syncOp['delete'] = true;
                    } else {
                        $syncOp['delete'] = false;
                    }

                    foreach($syncOp['ops'] as $collectionName=>$ops){
                        $syncModifiedSubjects[] = ModifiedSubject::create($syncOp['id'],array(),$ops, $this->tripod->getDBName(), $collectionName, $syncOp['delete']);
                    }
                }

                if(!empty($syncModifiedSubjects)){
                    $this->processSyncOperations($syncModifiedSubjects);
                }

                // now queue all async operations
                $asyncModifiedSubjects = array();
                foreach($operationsToPerform[OP_ASYNC] as $asyncOp){
                    if(in_array($asyncOp['id']['r'], $changes['deletedSubjects'])){
                        $asyncOp['delete'] = true;
                    } else {
                        $asyncOp['delete'] = false;
                    }

                    foreach($asyncOp['ops'] as $collectionName=>$ops){
                        $asyncModifiedSubjects[] = ModifiedSubject::create($asyncOp['id'],array(),$ops, $this->tripod->getDBName(), $collectionName, $asyncOp['delete']);
                    }
                }

                if(!empty($asyncModifiedSubjects)){
                    $this->queueASyncOperations($asyncModifiedSubjects);
                }
            }
        }
        catch(Exception $e){
            // ensure we reset the original read preference in the event of an exception
            $this->resetOriginalReadPreference();
            throw $e;
        }

        $this->resetOriginalReadPreference();

        return true;
    }

    /**
     * @param array $subjectsAndPredicatesOfChange
     * @param $contextAlias
     * @param $asyncConfig
     * @return array
     */
    protected function getApplicableOperations(Array $subjectsAndPredicatesOfChange, $contextAlias, Array $asyncConfig)
    {
        $filter = array();
        foreach(array_keys($subjectsAndPredicatesOfChange) as $s){
            $resourceAlias = $this->labeller->uri_to_alias($s);
            // build $filter for queries to impact index
            $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);
        }
        $query = array('_id'=>array('$in'=>$filter));
        $docs = $this->getCollection()->find($query, array('_id'=>true, 'rdf:type'=>true));

        if($docs->count() == 0 ) {
            return array();
        }

        $operations  = array();
        $operations[OP_SYNC]  = array();
        $operations[OP_ASYNC] = array();

        $viewTypes   = $this->config->getTypesInViewSpecifications($this->tripod->getCollectionName());
        $tableTypes  = $this->config->getTypesInTableSpecifications($this->tripod->getCollectionName());
        $searchTypes = $this->config->getTypesInSearchSpecifications($this->tripod->getCollectionName());

        foreach($docs as $doc)
        {
            $docResource = $doc['_id'][_ID_RESOURCE];
            $docContext  = $doc['_id'][_ID_CONTEXT];
            $docHash     = md5($docResource.$docContext);

            $docTypes = array();
            if(isset($doc["rdf:type"])) {
                if(isset($doc["rdf:type"][VALUE_URI])){
                    $docTypes[] = $doc["rdf:type"][VALUE_URI];
                } else {
                    foreach($doc["rdf:type"] as $t){
                        if(isset($t[VALUE_URI]))
                        {
                            $docTypes[] = $t[VALUE_URI];
                        }
                    }
                }
            }

            foreach($docTypes as $type)
            {
                if(in_array($type, $viewTypes)){
                    if($asyncConfig[OP_VIEWS] == true) {
                        if(!array_key_exists($docHash, $operations[OP_ASYNC])){
                            $operations[OP_ASYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_VIEWS);
                    } else{
                        if(!array_key_exists($docHash, $operations[OP_SYNC])){
                            $operations[OP_SYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_VIEWS);
                    }
                }

                if(in_array($type, $tableTypes) && empty($subjectsAndPredicatesOfChange[$docResource])) {
                    if($asyncConfig[OP_TABLES] == true) {
                        if(!array_key_exists($docHash, $operations[OP_ASYNC])){
                            $operations[OP_ASYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_TABLES);
                    } else{
                        if(!array_key_exists($docHash, $operations[OP_SYNC])){
                            $operations[OP_SYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_TABLES);
                    }
                }

                if(in_array($type, $searchTypes) && empty($subjectsAndPredicatesOfChange[$docResource])) {
                    if($asyncConfig[OP_SEARCH] == true) {
                        if(!array_key_exists($docHash, $operations[OP_ASYNC])){
                            $operations[OP_ASYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_ASYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_SEARCH);
                    } else{
                        if(!array_key_exists($docHash, $operations[OP_SYNC])){
                            $operations[OP_SYNC][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()] = array();
                        }
                        array_push($operations[OP_SYNC][$docHash]['ops'][$this->tripod->getCollectionName()], OP_SEARCH);
                    }
                }
            }
        }
        return $operations;
    }

    protected function getImpactedOperations(Array $subjectsAndPredicatesOfChange, $contextAlias, $asyncConfig)
    {
        $operations = array();
        $operations[OP_SYNC]  = array();
        $operations[OP_ASYNC] = array();

        foreach($this->tripod->getTripodViews()->findImpactedViews(array_keys($subjectsAndPredicatesOfChange), $contextAlias) as $doc) {
            $spec = $this->config->getViewSpecification($doc['_id']['type']);
            if(!empty($spec)){
                $fromCollection = $spec['from'];

                $docHash = md5($doc['_id']['r'] . $doc['_id']['c']);

                if($asyncConfig[OP_VIEWS] == true) {
                    if(!array_key_exists($docHash, $operations[OP_ASYNC])){
                        $operations[OP_ASYNC][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations[OP_ASYNC][$docHash]['ops'])) {
                        $operations[OP_ASYNC][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations[OP_ASYNC][$docHash]['ops'][$fromCollection], OP_VIEWS);

                } else {

                    if(!array_key_exists($docHash, $operations[OP_SYNC])){
                        $operations[OP_SYNC][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations[OP_SYNC][$docHash]['ops'])) {
                        $operations[OP_SYNC][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations[OP_SYNC][$docHash]['ops'][$fromCollection], OP_VIEWS);
                }
            }
        }

        foreach($this->tripod->getTripodTables()->findImpactedTableRows($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
            $spec = $this->config->getTableSpecification($doc[_ID_KEY][_ID_TYPE]);
            $fromCollection = $spec['from'];

            $docHash = md5($doc[_ID_KEY][_ID_RESOURCE] . $doc[_ID_KEY][_ID_CONTEXT] . $doc[_ID_KEY][_ID_TYPE]);

            $syncOrAsync = ($asyncConfig[OP_TABLES] ? OP_ASYNC : OP_SYNC);

            if(!array_key_exists($docHash, $operations[$syncOrAsync])){
                $operations[$syncOrAsync][$docHash] = array(
                    'id'=>array(
                        _ID_RESOURCE=>$doc[_ID_KEY][_ID_RESOURCE],
                        _ID_CONTEXT=>$doc[_ID_KEY][_ID_CONTEXT],
                        _ID_TYPE=>$doc[_ID_KEY][_ID_TYPE]
                    ),
                    'ops'=>array()
                );
            }
            if(!array_key_exists($fromCollection, $operations[$syncOrAsync][$docHash]['ops'])) {
                $operations[$syncOrAsync][$docHash]['ops'][$fromCollection] = array();
            }
            array_push($operations[$syncOrAsync][$docHash]['ops'][$fromCollection], OP_TABLES);
        }

        if($this->config->searchProvider !== null) {
            foreach($this->tripod->getSearchIndexer()->findImpactedSearchDocuments($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
                $spec = $this->config->getSearchDocumentSpecification($doc[_ID_KEY][_ID_TYPE]);
                $fromCollection = $spec['from'];

                $docHash = md5($doc[_ID_KEY][_ID_RESOURCE] . $doc[_ID_KEY][_ID_CONTEXT] . $doc[_ID_KEY][_ID_TYPE]);

                $syncOrAsync = ($asyncConfig[OP_SEARCH] ? OP_ASYNC : OP_SYNC);

                if(!array_key_exists($docHash, $operations[$syncOrAsync])){
                    $operations[$syncOrAsync][$docHash] = array(
                        'id'=>array(
                            _ID_RESOURCE=>$doc[_ID_KEY][_ID_RESOURCE],
                            _ID_CONTEXT=>$doc[_ID_KEY][_ID_CONTEXT],
                            _ID_TYPE=>$doc[_ID_KEY][_ID_TYPE]
                        ),
                        'ops'=>array()
                    );
                }
                if(!array_key_exists($fromCollection, $operations[$syncOrAsync][$docHash]['ops'])) {
                    $operations[$syncOrAsync][$docHash]['ops'][$fromCollection] = array();
                }
                array_push($operations[$syncOrAsync][$docHash]['ops'][$fromCollection], OP_SEARCH);
            }
        }
        // return an array of document ids with the operations we need to perform for each
        return $operations;
    }

    /**
     * Change the read preference to RP_PRIMARY
     * Used for a write operation
     */
    protected function setReadPreferenceToPrimary(){
        $currReadPreference = $this->getReadPreference();
        if($currReadPreference !== MongoClient::RP_PRIMARY){
            $this->originalReadPreference = $currReadPreference;
            $this->getCollection()->setReadPreference(MongoClient::RP_PRIMARY);
        }
    }


    /**
     * Get the current read preference
     *
     * @return array {@link http://www.php.net/manual/en/mongoclient.getreadpreference.php}
     */
    public function getReadPreference(){
        return $this->getCollection()->getReadPreference();
    }


    /**
     * Reset the original read preference after changing with setReadPreferenceToPrimary
     */
    protected function resetOriginalReadPreference(){
        // If the read preference has not been changed then simply return
        if($this->originalReadPreference === array()){
            return;
        }
        // Make the change.
        $preferencesTagsets = isset($this->originalReadPreference['tagsets']) ? $this->originalReadPreference['tagsets'] : array();
        $this->getCollection()->setReadPreference($this->originalReadPreference['type'], $preferencesTagsets);

        // Reset the orignal read preference var so we know it is back to normal
        $this->originalReadPreference = array();
    }

    /**
     * Ensure that the graph we want to persist has data with valid cardinality.
     *
     * @param ExtendedGraph $graph
     * @throws TripodCardinalityException
     */
    protected function validateGraphCardinality(ExtendedGraph $graph)
    {
        $config = MongoTripodConfig::getInstance();
        $cardinality = $config->getCardinality($this->tripod->getDBName(), $this->tripod->getCollectionName());
        $namespaces = $config->ns;
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
                throw new TripodCardinalityException("Namespace '{$namespace}' not defined for qname: {$qname}");
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
                        throw new TripodCardinalityException("Cardinality failed on {$subjectUri} for '{$qname}' - should only have 1 value and has: ".implode(', ', $v));
                    }
                }
            }
        }
    }


    /**
     * @param ChangeSet $cs Change-set to apply
     * @param array $subjectsOfChange array of subjects of change
     * @param $contextAlias string
     * @return array
     * @throws TripodException
     * @throws Exception
     */
    protected function storeChanges(ChangeSet $cs, Array $subjectsOfChange, $contextAlias)
    {
        $t = new Timer();
        $t->start();

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

            $this->tripod->getTransactionLog()->createNewTransaction($transaction_id, $csDoc['value'][_GRAPHS], $originalCBDs, $this->tripod->getDBName(), $this->tripod->getCollectionName());

            if(empty($originalCBDs)) // didn't get lock on documents
            {
                $this->tripod->getTransactionLog()->failTransaction($transaction_id, new Exception('Did not obtain locks on documents'));
                throw new Exception('Did not obtain locks on documents');
            }

            $changes = $this->applyChangeSet($cs,$originalCBDs,$contextAlias, $transaction_id);

            $this->tripod->debugLog(MONGO_LOCK,
                array(
                    'description'=>'MongoTripod::storeChanges - Unlocking documents, apply change-set completed',
                    'transaction_id'=>$transaction_id,
                )
            );

            $this->unlockAllDocuments($transaction_id);
            $this->tripod->getTransactionLog()->completeTransaction($transaction_id, $changes['newCBDs']);

            $t->stop();
            $this->tripod->timingLog(MONGO_WRITE, array('duration'=>$t->result(), 'subjectsOfChange'=>implode(", ",$subjectsOfChange)));
            $this->tripod->getStat()->timer(MONGO_WRITE.".{$this->tripod->getCollectionName()}",$t->result());

            return $changes;
        }
        catch(Exception $e)
        {
            $this->tripod->getStat()->increment(MONGO_ROLLBACK);
            $this->tripod->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'Save Failed Rolling back transaction:' . $e->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'subjectsOfChange'=>implode(",",$subjectsOfChange),
                    'mongoDriverError' => $this->tripod->db->lastError()
                )
            );
            $this->rollbackTransaction($transaction_id, $originalCBDs, $e);

            throw new TripodException('Error storing changes: '.$e->getMessage()." >>>" . $e->getTraceAsString());
        }
    }

    /**
     * @param $transaction_id string id of the transaction
     * @param $originalCBDs array containing the original CBDS
     * @param $exception
     * @return bool
     * @throws Exception
     */
    protected function rollbackTransaction($transaction_id, $originalCBDs, Exception $exception)
    {
        // set transaction to cancelling
        $this->tripod->getTransactionLog()->cancelTransaction($transaction_id, $exception);

        if (!empty($originalCBDs)) {  // restore the original CBDs
            foreach ($originalCBDs as $g)
            {
                $result = $this->updateCollection(array('_id' => $g['_id']), $g, array('w' => 1));
                if($result['err']!=NULL )
                {
                    // Error log here
                    $this->tripod->errorLog(MONGO_ROLLBACK,
                        array(
                            'description' => 'MongoTripod::rollbackTransaction - Error updating transaction',
                            'exception_message' => $exception->getMessage(),
                            'transaction_id' => $transaction_id,
                            'mongoDriverError' => $this->tripod->db->lastError()
                        )
                    );
                    throw new Exception("Failed to restore Original CBDS for transaction: {$transaction_id} stopped at ".$g['_id']);
                }
            }
        }
        else
        {
            $this->tripod->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'MongoTripod::rollbackTransaction - Unlocking documents',
                    'exception_message' => $exception->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'mongoDriverError' => $this->tripod->db->lastError()
                )
            );
        }
        $this->unlockAllDocuments($transaction_id);

        // set transaction to failed
        $this->tripod->getTransactionLog()->failTransaction($transaction_id);
        return true;
    }

    /**
     * @return string
     */
    protected function generateTransactionId()
    {
        return 'transaction_' . $this->getUniqId();
    }

    /**
     * @return string
     */
    protected function getUniqId()
    {
        return uniqid('', true);
    }


    /**
     * @param ChangeSet $cs
     * @param $originalCBDs
     * @param $contextAlias
     * @param $transaction_id
     * @return array
     * @throws Exception
     */
    protected function applyChangeSet(ChangeSet $cs, $originalCBDs,$contextAlias, $transaction_id)
    {
        if (preg_match('/^CBD_/',$this->getCollection()->getName()))
        {
            // how many subjects of change?
            /** @noinspection PhpParamsInspection */
            $changes = $cs->get_subjects_of_type($this->labeller->qname_to_uri("cs:ChangeSet"));

            // gather together all the updates (we'll apply them later)....
            $updates = array();
            $deletes = array();

            $upsert = false;

            $newCBDs = array();

            foreach ($changes as $change)
            {
                $subjectOfChange = $cs->get_first_resource($change,$this->labeller->qname_to_uri("cs:subjectOfChange"));
                $criteria = array(
                    "_id"=>array(_ID_RESOURCE=>$this->labeller->uri_to_alias($subjectOfChange),_ID_CONTEXT=>$contextAlias)
                );

                // read before write, and to find array indexes and get document in memory
                //$targetGraph = new MongoGraph();

                $doc = $this->getDocumentForUpdate($subjectOfChange, $contextAlias, $originalCBDs);

                $targetGraph = new MongoGraph();
                $targetGraph->add_tripod_array($doc);

                if(!$targetGraph->has_triples_about($subjectOfChange))
                {
                    $upsert = true;
                }

                // add the old vals to critera
                $removals = $cs->get_subject_property_values($change,$this->labeller->qname_to_uri("cs:removal"));
                $additions = $cs->get_subject_property_values($change,$this->labeller->qname_to_uri("cs:addition"));

                foreach ($removals as $r)
                {
                    $predicate = $cs->get_first_resource($r["value"],$this->labeller->qname_to_uri("rdf:predicate"));
                    $object = $cs->get_subject_property_values($r["value"],$this->labeller->qname_to_uri("rdf:object"));

                    $isUri = ($object[0]['type']=="uri");

                    // is $predicate in $docs already?
                    $valueExists = ($isUri) ? $targetGraph->has_resource_triple($subjectOfChange,$predicate,$object[0]["value"]) : $targetGraph->has_literal_triple($subjectOfChange,$predicate,$object[0]["value"]);

                    if (!$valueExists)
                    {
                        $this->tripod->errorLog("Removal value {$subjectOfChange} {$predicate} {$object[0]['value']} does not appear in target document to be updated",array("targetGraph"=>$targetGraph->to_ntriples()));
                        throw new Exception("Removal value {$subjectOfChange} {$predicate} {$object[0]['value']} does not appear in target document to be updated");
                    }
                    else if ($isUri)
                    {
                        $targetGraph->remove_resource_triple($subjectOfChange,$predicate,$object[0]["value"]);

                    }
                    else
                    {
                        $targetGraph->remove_literal_triple($subjectOfChange,$predicate,$object[0]["value"]);

                    }

                    //$criteria[$targetGraph->uri_to_qname($predicate)] = array('$elemMatch'=>$object[0]);
                }

                foreach ($additions as $r)
                {
                    $predicate = $cs->get_first_resource($r["value"],$this->labeller->qname_to_uri("rdf:predicate"));
                    $object = $cs->get_subject_property_values($r["value"],$this->labeller->qname_to_uri("rdf:object"));

                    $isUri = ($object[0]['type']=="uri");

                    if ($isUri)
                    {
                        $targetGraph->add_resource_triple($subjectOfChange,$predicate,$object[0]["value"]);
                    }
                    else
                    {
                        $targetGraph->add_literal_triple($subjectOfChange,$predicate,$object[0]["value"]);
                    }
                }

                //todo: criteria at this point should probably include all removal statements if they exist
                // i.e. we only want to update document if it has all these values ( think platform 409 )
                // currently the only criteria is the doc id
                //var_dump($targetGraph->to_tripod_array($subjectOfChange));

                $_new_version = 0;
                if(isset($doc[_VERSION]))
                {
                    $_version = (int)$doc[_VERSION];
                    $criteria[_VERSION] = $_version;
                    $_new_version = $_version + 1;
                }

                // update datestamps
                $_updated_ts = new MongoDate();

                if($targetGraph->is_empty())
                {
                    $newDocument = array('_id'=>array(_ID_RESOURCE=>$this->labeller->uri_to_alias($subjectOfChange),_ID_CONTEXT=>$contextAlias), _VERSION=>$_new_version, _UPDATED_TS=>$_updated_ts);
                    if(isset($doc[_CREATED_TS])) { // make sure when doc is deleted and it had a created date, we preserve it.
                        $newDocument[_CREATED_TS] = $doc[_CREATED_TS];
                    }

                    array_push($newCBDs, $newDocument);
                    $deletes[] = array("criteria"=>$criteria, 'change'=>$newDocument);
                }
                else
                {
                    $newDocument = $targetGraph->to_tripod_array($subjectOfChange,$contextAlias);
                    $newDocument[_VERSION] = $_new_version;
                    $newDocument[_UPDATED_TS] = $_updated_ts;
                    if($_new_version == 0) {
                        $newDocument[_CREATED_TS] = new MongoDate();
                    } else {
                        if(isset($doc[_CREATED_TS])) {
                            $newDocument[_CREATED_TS] = $doc[_CREATED_TS];
                        }
                    }
                    array_push($newCBDs, $newDocument);
                    $updates[] = array("criteria"=>$criteria,"change"=>$newDocument);
                }
            }

            // apply each update
            foreach ($updates as $update)
            {
                $command = array(
                    "findAndModify" => $this->getCollection()->getName(),
                    "query" => $update['criteria'],
                    "update" => $update['change'],
                    "upsert" => $upsert,
                    "new"=>true
                );

                try{
                    $result = $this->tripod->db->command($command);
                } catch (Exception $e) {

                    $this->tripod->errorLog(MONGO_WRITE,
                        array(
                            'description'=>'Error with Mongo DB command:' . $e->getMessage(),
                            'transaction_id'=>$transaction_id,
                            'mongoDriverError' => $this->tripod->db->lastError()
                        )
                    );
                    throw new Exception($e);
                }

                if (!$result["ok"])
                {
                    $this->tripod->errorLog("Update failed with err.", $result);
                    throw new Exception("Update failed with err {$result['err']}");
                }

                if($result['value']==null)
                {
                    $this->tripod->errorLog(MONGO_WRITE,
                        array(
                            'description'=>'MongoTripod::storeChanges - Update failed we did not find a matching document (transaction_id - ' .$transaction_id .')',
                            $result
                        )
                    );
                    throw new Exception("Update failed we did not find a matching document");
                }
            }

            foreach($deletes as $delete)
            {
                $command = array(
                    "findAndModify" => $this->getCollection()->getName(),
                    "query" => $delete['criteria'],
                    "update" => $delete['change'],
                    "upsert" => false,
                    "new"=>false
                );

                $result = $this->tripod->db->command($command);

                if (!$result["ok"])
                {
                    $this->tripod->errorLog("Delete failed with err.", $result);
                    throw new Exception("Delete failed with err {$result['err']}");
                }

                if($result['value']==null)
                {
                    $this->tripod->errorLog("Delete failed we did not find a matching document.", $result);
                    throw new Exception("Delete failed we did not find a matching document");
                }

            }

            $updatedSubjects = array();
            $deletedSubjects = array();
            foreach($updates as $u)
            {
                $updatedSubjects[] = $u['criteria']['_id'][_ID_RESOURCE];
            }

            foreach($deletes as $d)
            {
                $deletedSubjects[] = $d['criteria']['_id'][_ID_RESOURCE];
            }

            $retval = array();
            $retval['newCBDs'] = $newCBDs;
            $retval['updatedSubjects'] = $updatedSubjects;
            $retval['deletedSubjects'] = $deletedSubjects;

            return $retval;
        }
        else
        {
            throw new Exception("Attempted to update a non-CBD collection");
        }
    }

    /**
     * Given a set of CBD's return the CBD that matches the Subject of Change
     * @param $subjectOfChange
     * @param $contextAlias
     * @param $cbds
     * @return null | array the document from the collection of $cbds that matches the subject of change
     */
    protected function getDocumentForUpdate($subjectOfChange, $contextAlias, $cbds)
    {
        foreach($cbds as $c)
        {
            if($c["_id"]==array(_ID_RESOURCE=>$this->labeller->uri_to_alias($subjectOfChange),_ID_CONTEXT=>$contextAlias))
            {
                return $c;
                break;
            }
        }

        return null;
    }

    /**
     * @param array $modifiedSubjects
     */
    protected function processSyncOperations(Array $modifiedSubjects)
    {
        foreach($modifiedSubjects as $subject)
        {
            /* @var $subject ModifiedSubject */
            $data = $subject->getData();
            $operations = $data['operations'];
            foreach ($operations as $op)
            {
                if($data['collection'] == $this->tripod->getCollectionName()){
                    $observer = $this->tripod->getObserver($op);
                } else {
                    $observer =$this->getMongoTripod($data)->getObserver($op);
                }
                $subject->attach($observer);
            }
            $t = new Timer();
            $t->start();

            $subject->notify();

            $t->stop();
            $this->tripod->timingLog(MONGO_ON_THE_FLY_MR,array(
                "duration"=>$t->result(),
                "operations"=>var_export($data['operations'],true),
                "database"=>$data['database'],
                "collection"=>$data['collection'],
                "resource"=>$data['r']
            ));
            $this->tripod->getStat()->timer(MONGO_ON_THE_FLY_MR,$t->result());
        }
    }


    /////////////////////////// QUEUE RELATED METHODS BELOW HERE ///////////////////////////////////////


    protected function queueASyncOperations(Array $modifiedSubjects)
    {
        foreach ($modifiedSubjects as $subject)
        {
            /* @var $subject ModifiedSubject */
            $data = $subject->getData();
            $this->tripod->debugLog(MONGO_ADD_TO_QUEUE,array(
                    "operations"=>var_export($data['operations'],true),
                    "database"=>$data['database'],
                    "collection"=>$data['collection'],
                    "resource"=>$data['r']
                )
            );
            $this->tripod->getStat()->increment(MONGO_ADD_TO_QUEUE);
            $this->getQueue()->addItem($subject);
        }
    }

    public function getQueue()
    {
        if(empty($this->queue))
        {
            $this->queue = new MongoTripodQueue();
        }

        return $this->queue;
    }

    //////// LOCKS \\\\\\\\

    /**
     * Get locked documents for a date range or all documents if no date range is given
     * @param $fromDateTime String
     * @param $tillDateTime String
     * @return array
     */
    public function getLockedDocuments($fromDateTime = null , $tillDateTime = null)
    {
        $query = array();
        if(!empty($fromDateTime) || !empty($tillDateTime)){
            $query[_LOCKED_FOR_TRANS_TS] = array();

            if(!empty($fromDateTime)) $query[_LOCKED_FOR_TRANS_TS]['$gte'] = new MongoDate(strtotime($fromDateTime));
            if(!empty($tillDateTime)) $query[_LOCKED_FOR_TRANS_TS]['$lte'] = new MongoDate(strtotime($tillDateTime));
        }
        $docs = $this->lCollection->find($query)->sort(array(_LOCKED_FOR_TRANS => 1));

        if($docs->count() == 0 ) {
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
     * @param $subjectsOfChange array of the subjects that are part of this transaction
     * @param $transaction_id string id for this transaction
     * @param $contextAlias string
     * @return array|null returns an array of CBDs, each CBD is the version at the time at which the lock was attained
     * @throws Exception
     */
    protected function lockAllDocuments($subjectsOfChange, $transaction_id, $contextAlias)
    {
        for($retry=1; $retry <= $this->retriesToGetLock; $retry++)
        {
            $originalCBDs = array();
            $lockedSubjects = array();
            foreach ($subjectsOfChange as $s)
            {
                $this->tripod->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'MongoTripod::lockAllDocuments - Attempting to get lock',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'attempt' => $retry
                    )
                );

                $document = $this->lockSingleDocument($s, $transaction_id, $contextAlias);
                if(!empty($document)){

                    $this->tripod->debugLog(MONGO_LOCK,
                        array(
                            'description'=>'MongoTripod::lockAllDocuments - Got the lock',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'retry' => $retry
                        )
                    );

                    $this->tripod->getStat()->increment(MONGO_LOCK);
                    $originalCBDs[] = $document;
                    $lockedSubjects[] = $s;
                }
            }

            if(count($subjectsOfChange) == count($lockedSubjects)){ //if all subjects of change locked, we are good.
                return $originalCBDs;
            }else{

                if(count($lockedSubjects)) //If any subject was locked, unlock it
                $this->unlockAllDocuments($transaction_id);

                $this->tripod->debugLog(MONGO_LOCK,
                    array(
                        'description'=>"MongoTripod::lockAllDocuments - Unable to lock all ". count($subjectsOfChange) ."  documents, unlocked  " . count($lockedSubjects) . " locked documents",
                        'transaction_id'=>$transaction_id,
                        'documentsToLock' => implode(",", $subjectsOfChange),
                        'documentsLocked' => implode(",", $lockedSubjects),
                        'retry' => $retry
                    )
                );
                $n = mt_rand (25,40); usleep($n*1000); //do a retry
            }
        }

        $this->tripod->errorLog(MONGO_LOCK,
            array(
                'description'=>'Unable to lock all required documents. Exhausted retries',
                'retries' => $this->retriesToGetLock,
                'transaction_id'=>$transaction_id,
                'subjectsOfChange'=>implode(", ",$subjectsOfChange),
                'mongoDriverError' => $this->tripod->db->lastError()
            )
        );
        return NULL;
    }

    /**
     * Remove locks that are there forever, creates a audit entry to keep track who and why removed these locks.
     * @param $transaction_id
     * @param $reason
     * @return bool
     * @throws Exception, if something goes wrong when unlocking documents, or creating audit entries.
     */
    public function removeInertLocks($transaction_id, $reason)
    {
        $docs = $this->lCollection->find(array(_LOCKED_FOR_TRANS => $transaction_id));

        if($docs->count() == 0 ) {
            return false;
        }else{

            //1. Create audit entry with in_progress status
            $auditCollection  = $this->getAuditManualRollbacksCollection();
            $auditDocumentId = $this->generateIdForNewMongoDocument();
            try{
                $documents = array();
                foreach($docs as $doc)
                {
                    $documents[] = $doc['_id']['r'];
                }

                $result = $auditCollection->insert(
                    array(
                        '_id' => $auditDocumentId,
                        'type' => AUDIT_TYPE_REMOVE_INERT_LOCKS,
                        'status' => AUDIT_STATUS_IN_PROGRESS,
                        'reason' => $reason,
                        'transaction_id' => $transaction_id,
                        'documents' => $documents,
                        _CREATED_TS=> $this->getMongoDate(),
                    )
                );
                if(!$result["ok"] || $result['err']!=NULL){
                    throw new Exception("Failed to create audit entry with error message- " . $result['err']);
                }
            }
            catch(Exception $e) { //simply send false as status as we are unable to create audit entry
                $this->tripod->errorLog(MONGO_LOCK,
                    array(
                        'description'=>'MongoTripod::removeInertLocks - failed',
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
                $result = $auditCollection->update(array("_id" => $auditDocumentId), array('$set' => array("status" => AUDIT_STATUS_COMPLETED, _UPDATED_TS => $this->getMongoDate())));
                if($result['err']!=NULL )
                {
                    throw new Exception("Failed to update audit entry with error message- " . $result['err']);
                }
            }
            catch(Exception $e) {
                $logInfo = array(
                    'description'=>'MongoTripod::removeInertLocks - failed',
                    'transaction_id'=>$transaction_id,
                    'exception-message' => $e->getMessage()
                );

                //4. Update audit entry to say it was failed with error
                $result = $auditCollection->update(array("_id" => $auditDocumentId), array('$set' => array("status" => AUDIT_STATUS_ERROR, _UPDATED_TS => $this->getMongoDate(), 'error' => $e->getMessage())));

                if($result['err']!=NULL )
                {
                    $logInfo['additional-error']=  "Failed to update audit entry with error message- " . $result['err'];
                }

                $this->tripod->errorLog(MONGO_LOCK, $logInfo);
                throw $e;
            }
        }
        return true;
    }

    /**
     * Unlocks documents locked by current transaction
     * @param $transaction_id string id for this transaction
     * @return bool
     * @throws Exception is thrown if for any reason the update to mongo fails
     */
    protected function unlockAllDocuments($transaction_id)
    {
        $res = $this->lCollection->remove(array(_LOCKED_FOR_TRANS => $transaction_id), array('w' => 1));

        // I can't check $res['n']>0 here, because same method is called in rollback where there might be no locked subjects at all
        if(!$res["ok"] || $res['err']!=NULL){
            $this->tripod->errorLog(MONGO_LOCK,
                array(
                    'description'=>'MongoTripod::unlockAllDocuments - Failed to unlock documents (transaction_id - ' .$transaction_id .')',
                    'mongoDriverError' => $this->tripod->db->lastError(),
                    $res
                )
            );
            throw new Exception("Failed to unlock documents as part of transaction : ".$transaction_id);
        }
        return true;
    }


    /**
     * @param $s
     * @param $transaction_id
     * @param $contextAlias
     * @return array
     */
    protected function lockSingleDocument($s, $transaction_id, $contextAlias)
    {
        $countEntriesInLocksCollection = $this->lCollection->count(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));

        if($countEntriesInLocksCollection > 0) //Subject is already locked
        return false;
        else{
            try{ //Add a entry to locks collection for this subject, will throws exception if an entry already there
                $result = $this->lCollection->insert(
                    array(
                        '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias),
                        _LOCKED_FOR_TRANS => $transaction_id,
                        _LOCKED_FOR_TRANS_TS=>new MongoDate()
                    ),
                    array("w" => 1)
                );

                if(!$result["ok"] || $result['err']!=NULL){
                    throw new Exception("Failed to lock document with error message- " . $result['err']);
                }
            }
            catch(Exception $e) { //Subject is already locked or unable to lock
                $this->tripod->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'MongoTripod::lockSingleDocument - failed with exception',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'exception-message' => $e->getMessage()
                    )
                );
                return false;
            }

            //Let's get original document for processing.
            $document  = $this->getCollection()->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
            if(empty($document)){ //if document is not there, create it
                try{
                    $result = $this->getCollection()->insert(
                        array(
                            '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)
                        ),
                        array("w" => 1)
                    );

                    if(!$result["ok"] || $result['err']!=NULL){
                        throw new Exception("Failed to create new document with error message- " . $result['err']);
                    }
                    $document  = $this->getCollection()->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
                }
                catch(Exception $e){
                    $this->tripod->errorLog(MONGO_LOCK,
                        array(
                            'description'=>'MongoTripod::lockSingleDocument - failed when creating new document',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'exception-message' => $e->getMessage(),
                            'mongoDriverError' => $this->tripod->db->lastError()
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
     * @return MongoCollection
     */
    protected function getAuditManualRollbacksCollection()
    {
        return $this->tripod->db->selectCollection(AUDIT_MANUAL_ROLLBACKS_COLLECTION);
    }
    
    /**
     * For mocking
     * @return MongoTripodConfig
     */
    protected function getMongoTripodConfigInstance()
    {
        return MongoTripodConfig::getInstance();
    }

    /**
     * @return MongoId
     */
    protected function generateIdForNewMongoDocument()
    {
        return new MongoId();
    }

    /**
     * @return MongoDate
     */
    protected function getMongoDate()
    {
        return new MongoDate();
    }
    
    protected function getCollection()
    {
        return $this->tripod->collection;
    }


    public function applyTransaction(Array $transaction)
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
                if($n['_id'][_ID_RESOURCE]==$s)
                {
                    $this->updateCollection(array('_id'=>$n['_id']), $n, array('upsert'=>true));
                    break;
                }
            }
        }
    }

    protected function getMongoTripod($data) {
        return new MongoTripod(
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
        return $this->getCollection()->update($query, $update, $options);
    }

    protected function getContextAlias($context=null)
    {
        $contextAlias = $this->labeller->uri_to_alias((empty($context)) ? $this->defaultContext : $context);
        return (empty($contextAlias)) ? MongoTripodConfig::getInstance()->getDefaultContextAlias() : $contextAlias;
    }
}