<?php
require_once TRIPOD_DIR.'classes/Timer.class.php';
require_once TRIPOD_DIR.'exceptions/TripodException.class.php';
require_once TRIPOD_DIR.'exceptions/TripodSearchException.class.php';
require_once TRIPOD_DIR.'exceptions/TripodCardinalityException.class.php';
require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR.'mongo/MongoGraph.class.php';
require_once TRIPOD_DIR.'mongo/ModifiedSubject.class.php';
require_once TRIPOD_DIR.'mongo/base/MongoTripodBase.class.php';
require_once TRIPOD_DIR.'mongo/delegates/MongoTransactionLog.class.php';
require_once TRIPOD_DIR.'mongo/delegates/MongoTripodViews.class.php';
require_once TRIPOD_DIR.'mongo/delegates/MongoTripodTables.class.php';
require_once TRIPOD_DIR.'mongo/delegates/MongoTripodSearchIndexer.class.php';
require_once TRIPOD_DIR.'mongo/queue/MongoTripodQueue.class.php';
require_once TRIPOD_DIR.'ITripod.php';
require_once TRIPOD_DIR.'classes/ChangeSet.class.php';

/** @noinspection PhpIncludeInspection */

$TOTAL_TIME=0;

class MongoTripod extends MongoTripodBase implements ITripod
{
    /**
     * @var MongoCollection
     */
    public $lCollection;

    /**
     * $var MongoTransactionLog
     */
    private $transaction_log = null;

    /**
     * @var MongoTripodViews
     */
    private $tripod_views = null;

    /**
     * @var MongoTripodTables
     */
    private $tripod_tables = null;

    /**
     * @var MongoTripodSearchIndexer
     */
    private $search_indexer = null;

    /**
     * @var MongoTripodQueue
     */
    private $queue = null;

    /**
     * @var array The original read preference gets stored here
     *            when changing for a write.
     */
    private $originalReadPreference = array();

    /**
     * @var array
     */
    private $async = null;

    /**
     * @var Integer
     */
    private $retriesToGetLock;

    /**
     * Constructor for MongoTripod
     *
     * @param string $collectionName
     * @param string $dbName
     * @param array $opts an Array of options: <ul>
     * <li>defaultContext: (string) to use where a specific default context is not defined. Default is Null</li>
     * <li>async: (array) determines the async behaviour of views, tables and search. For each of these array keys, if set to true, generation of these elements will be done asyncronously on save. Default is array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true)</li>
     * <li>stat: this sets the stats object to use to record statistics around operations performed by Tripod. Default is null</li>
     * <li>readPreference: The Read preference to set for Mongo: Default is Mongo:RP_PRIMARY_PREFERRED</li>
     * <li>retriesToGetLock: Retries to do when unable to get lock on a document, default is 20</li></ul>
     * @throws Exception
     */
    public function __construct(
        $collectionName=MONGO_MAIN_COLLECTION,
        $dbName=MONGO_MAIN_DB,
        $opts=array()
    )
    {
        $opts = array_merge(array(
                'defaultContext'=>null,
                'async'=>array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true),
                'stat'=>null,
                'readPreference'=>MongoClient::RP_PRIMARY_PREFERRED,
                'retriesToGetLock' => 20)
            ,$opts);
        $this->collectionName = $collectionName;
        $this->dbName = $dbName;

        $this->config = $this->getMongoTripodConfigInstance();

        $this->labeller = $this->getLabeller();

        // default context
        $this->defaultContext = $opts['defaultContext'];

        //max retries to get lock
        $this->retriesToGetLock = $opts['retriesToGetLock'];

        // connect
        /* @var $m MongoClient */
        $connectionOptions = array('connectTimeoutMS'=>20000); // set a 20 second timeout on establishing a connection
        if($this->config->isReplicaSet($dbName)) {
            $connectionOptions['replicaSet'] = $this->config->getReplicaSetName($dbName);
        }
        try {
            $m = new MongoClient($this->config->getConnStr($dbName), $connectionOptions);
        } catch (Exception $e) {
            $this->getStat()->increment(MONGO_CONNECTION_ERROR);
            throw $e;
        }
        $m->setReadPreference($opts['readPreference']);
        // select a database
        $this->db = $m->selectDB($dbName);
        if (!empty($collectionName))
        {
            $this->collection = $this->db->selectCollection($collectionName);
        }

        //select locks collection
        $this->lCollection = $this->db->selectCollection(LOCKS_COLLECTION);

        // fill in and default any missing keys for $async array
        $async = $opts['async'];
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

    /**
     * Pass a subject to $resource and have mongo return a DESCRIBE <?resource>
     * @param $resource
     * @param $context
     * @return MongoGraph
     */
    public function describeResource($resource,$context=null)
    {
        $resource = $this->labeller->uri_to_alias($resource);
        $query = array(
            "_id" => array(
                _ID_RESOURCE=>$resource,
                _ID_CONTEXT=>$this->getContextAlias($context)));
        return $this->fetchGraph($query,MONGO_DESCRIBE);
    }

    /**
     * Pass subjects as to $resources and have mongo return a DESCRIBE <?resource[0]> <?resource[1]> <?resource[2]> etc.
     * @param array $resources
     * @param null $context
     * @return MongoGraph
     */
    public function describeResources(Array $resources,$context=null)
    {
        $ids = array();
        foreach ($resources as $resource)
        {
            $resource = $this->labeller->uri_to_alias($resource);
            $ids[] = array(
                _ID_RESOURCE=>$resource,
                _ID_CONTEXT=>$this->getContextAlias($context));
        }
        $query = array("_id" => array('$in' => $ids));
        return $this->fetchGraph($query,MONGO_MULTIDESCRIBE);
    }

    public function getViewForResource($resource, $viewType)
    {
        return $this->getTripodViews()->getViewForResource($resource,$viewType);
    }

    public function getViewForResources(Array $resources, $viewType)
    {
        return $this->getTripodViews()->getViewForResources($resources,$viewType);
    }

    public function getViews(Array $filter, $viewType)
    {
        return $this->getTripodViews()->getViews($filter,$viewType);
    }

    public function getTableRows($tableType, $filter = array(), $sortBy = array(), $offset = 0, $limit = 10)
    {
        return $this->getTripodTables()->getTableRows($tableType,$filter,$sortBy,$offset,$limit);
    }

    public function generateTableRows($tableType, $resource = null, $context = null)
    {
        $this->tripod_tables->generateTableRows($tableType,$resource,$context);
    }

    /**
     * @param string $tableType
     * @param string $fieldName
     * @param array $filter
     * @return array
     */
    public function getDistinctTableColumnValues($tableType, $fieldName, array $filter = array())
    {
        return $this->getTripodTables()->distinct($tableType, $fieldName, $filter);
    }

    /**
     * Get the current read preference
     *
     * @return array {@link http://www.php.net/manual/en/mongoclient.getreadpreference.php}
     */
    public function getReadPreference(){
        return $this->collection->getReadPreference();
    }

    /**
     * Change the read preference to RP_PRIMARY
     * Used for a write operation
     */
    protected function setReadPreferenceToPrimary(){
        $currReadPreference = $this->getReadPreference();
        if($currReadPreference !== MongoClient::RP_PRIMARY){
            $this->originalReadPreference = $currReadPreference;
            $this->collection->setReadPreference(MongoClient::RP_PRIMARY);
        }
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
        $this->collection->setReadPreference($this->originalReadPreference['type'], $preferencesTagsets);

        // Reset the orignal read preference var so we know it is back to normal
        $this->originalReadPreference = array();
    }

    /**
     * Create and apply a changeset which is the delta between $oldGraph and $newGraph
     * @param ExtendedGraph $oldGraph
     * @param ExtendedGraph $newGraph
     * @param $context
     * @param null $description
     * @return bool
     * @throws TripodException
     */
    public function saveChanges(
        ExtendedGraph $oldGraph,
        ExtendedGraph $newGraph,
        $context=null,
        $description=null)
    {
        $this->setReadPreferenceToPrimary();
        try{
            $contextAlias = $this->getContextAlias($context);

            if (!$this->config->isCollectionWithinConfig($this->dbName,$this->collectionName))
            {
                throw new TripodException("database:collection {$this->dbName}:{$this->collectionName} is not referenced within config, so cannot be written to");
            }

            $this->validateGraphCardinality($newGraph);

            $oldIndex = $oldGraph->get_index();
            $newIndex = $newGraph->get_index();
            $args = array('before' => $oldIndex, 'after' => $newIndex, 'changeReason' => $description);
            $cs = new ChangeSet($args);

            //file_put_contents("/tmp/cs.rdf.xml", $cs->to_rdfxml());

            if ($cs->has_changes())
            {
                // how many subjects of change?
                $subjectsOfChange = array();
                $subjectsAndPredicatesOfChange = array();
                /** @noinspection PhpParamsInspection */
                $changes = $cs->get_subjects_of_type($oldGraph->qname_to_uri("cs:ChangeSet"));
                foreach ($changes as $change)
                {
//                    $subjectsOfChange[] = $cs->get_first_resource($change,$oldGraph->qname_to_uri("cs:subjectOfChange"));
                    $subject = $cs->get_first_resource($change,$oldGraph->qname_to_uri("cs:subjectOfChange"));
                    if(!isset($subjectsAndPredicatesOfChange[$subject]))
                    {
                        $subjectsAndPredicatesOfChange[$subject] = array();
                    }
                    foreach($cs->get_subject_properties($subject, true) as $property)
                    {
                        $subjectsAndPredicatesOfChange[$subject][] = $property;
                    }
                }

//                $subjectsOfChange = array_unique($subjectsOfChange);
                $changes = $this->storeChanges($cs, array_keys($subjectsAndPredicatesOfChange),$contextAlias);

                // calculate what operations need performing, based on the subjects and anything they impact
                $operationsToPerform  = $this->getApplicableOperations(array_keys($subjectsAndPredicatesOfChange), $contextAlias, $this->async);

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
                foreach($operationsToPerform['sync'] as $syncOp){
                    if(in_array($syncOp['id']['r'], $changes['deletedSubjects'])){
                        $syncOp['delete'] = true;
                    } else {
                        $syncOp['delete'] = false;
                    }

                    foreach($syncOp['ops'] as $collectionName=>$ops){
                        $syncModifiedSubjects[] = ModifiedSubject::create($syncOp['id'],array(),$ops, $this->dbName, $collectionName, $syncOp['delete']);
                    }
                }

                if(!empty($syncModifiedSubjects)){
                    $this->processSyncOperations($syncModifiedSubjects);
                }

                // now queue all async operations
                $asyncModifiedSubjects = array();
                foreach($operationsToPerform['async'] as $asyncOp){
                    if(in_array($asyncOp['id']['r'], $changes['deletedSubjects'])){
                        $asyncOp['delete'] = true;
                    } else {
                        $asyncOp['delete'] = false;
                    }

                    foreach($asyncOp['ops'] as $collectionName=>$ops){
                        $asyncModifiedSubjects[] = ModifiedSubject::create($asyncOp['id'],array(),$ops, $this->dbName, $collectionName, $asyncOp['delete']);
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
                $this->errorLog(MONGO_LOCK,
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

                $this->errorLog(MONGO_LOCK, $logInfo);
                throw $e;
            }
        }
        return true;
    }

    protected function getImpactedOperations(Array $subjectsAndPredicatesOfChange, $contextAlias, $asyncConfig)
    {
        $operations = array();
        $operations['sync']  = array();
        $operations['async'] = array();

        foreach($this->getTripodViews()->findImpactedViews(array_keys($subjectsAndPredicatesOfChange), $contextAlias) as $doc) {
            $spec = $this->config->getViewSpecification($doc['_id']['type']);
            if(!empty($spec)){
                $fromCollection = $spec['from'];

                $docHash = md5($doc['_id']['r'] . $doc['_id']['c']);

                if($asyncConfig[OP_VIEWS] == true) {
                    if(!array_key_exists($docHash, $operations['async'])){
                        $operations['async'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations['async'][$docHash]['ops'])) {
                        $operations['async'][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations['async'][$docHash]['ops'][$fromCollection], OP_VIEWS);

                } else {

                    if(!array_key_exists($docHash, $operations['sync'])){
                        $operations['sync'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations['sync'][$docHash]['ops'])) {
                        $operations['sync'][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations['sync'][$docHash]['ops'][$fromCollection], OP_VIEWS);
                }
            }
        }

        foreach($this->getTripodTables()->findImpactedTableRows($subjectsAndPredicatesOfChange, $contextAlias) as $doc) {
            $spec = $this->config->getTableSpecification($doc['_id']['type']);
            $fromCollection = $spec['from'];

            $docHash = md5($doc['_id']['r'] . $doc['_id']['c']);

            if($asyncConfig[OP_TABLES] == true) {
                if(!array_key_exists($docHash, $operations['async'])){
                    $operations['async'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                }
                if(!array_key_exists($fromCollection, $operations['async'][$docHash]['ops'])) {
                    $operations['async'][$docHash]['ops'][$fromCollection] = array();
                }

                array_push($operations['async'][$docHash]['ops'][$fromCollection], OP_TABLES);
            } else {
                if(!array_key_exists($docHash, $operations['sync'])){
                    $operations['sync'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                }
                if(!array_key_exists($fromCollection, $operations['sync'][$docHash]['ops'])) {
                    $operations['sync'][$docHash]['ops'][$fromCollection] = array();
                }

                array_push($operations['sync'][$docHash]['ops'][$fromCollection], OP_TABLES);
            }
        }

        if($this->config->searchProvider !== null) {
            foreach($this->getSearchIndexer()->findImpactedSearchDocuments(array_keys($subjectsAndPredicatesOfChange), $contextAlias) as $doc) {
                $spec = $this->config->getSearchDocumentSpecification($doc['_id']['type']);
                $fromCollection = $spec['from'];

                $docHash = md5($doc['_id']['r'] . $doc['_id']['c']);

                if($asyncConfig[OP_SEARCH] == true) {
                    if(!array_key_exists($docHash, $operations['async'])){
                        $operations['async'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations['async'][$docHash]['ops'])) {
                        $operations['async'][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations['async'][$docHash]['ops'][$fromCollection], OP_SEARCH);
                } else {
                    if(!array_key_exists($docHash, $operations['sync'])){
                        $operations['sync'][$docHash] = array('id'=>array('r'=>$doc['_id']['r'], 'c'=>$doc['_id']['c']), 'ops'=>array());
                    }
                    if(!array_key_exists($fromCollection, $operations['sync'][$docHash]['ops'])) {
                        $operations['sync'][$docHash]['ops'][$fromCollection] = array();
                    }

                    array_push($operations['sync'][$docHash]['ops'][$fromCollection], OP_SEARCH);
                }
            }
        }
        // return an array of document ids with the operations we need to perform for each
        return $operations;
    }

    protected function getApplicableOperations(Array $subjectsOfChange, $contextAlias, $asyncConfig)
    {
        $filter = array();
        foreach(array_keys($subjectsOfChange) as $s){
            $resourceAlias = $this->labeller->uri_to_alias($s);
            // build $filter for queries to impact index
            $filter[] = array("r"=>$resourceAlias,"c"=>$contextAlias);
        }
        $query = array('_id'=>array('$in'=>$filter));
        $docs = $this->collection->find($query, array('_id'=>true, 'rdf:type'=>true));

        if($docs->count() == 0 ) {
            return array();
        }

        $operations  = array();
        $operations['sync']  = array();
        $operations['async'] = array();

        $viewTypes   = $this->config->getTypesInViewSpecifications($this->getCollectionName());
        $tableTypes  = $this->config->getTypesInTableSpecifications($this->getCollectionName());
        $searchTypes = $this->config->getTypesInSearchSpecifications($this->getCollectionName());

        foreach($docs as $doc)
        {
            $docResource = $doc['_id']['r'];
            $docContext  = $doc['_id']['c'];
            $docHash     = md5($docResource.$docContext);

            $docTypes = array();
            if(isset($doc["rdf:type"])) {
                if(isset($doc["rdf:type"]["u"])){
                    $docTypes[] = $doc["rdf:type"]["u"];
                } else {
                    foreach($doc["rdf:type"] as $t){
                        $docTypes[] = $t['u'];
                    }
                }
            }

            foreach($docTypes as $type)
            {
                if(in_array($type, $viewTypes)){
                    if($asyncConfig[OP_VIEWS] == true) {
                        if(!array_key_exists($docHash, $operations['async'])){
                            $operations['async'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['async'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['async'][$docHash]['ops'][$this->getCollectionName()], OP_VIEWS);
                    } else{
                        if(!array_key_exists($docHash, $operations['sync'])){
                            $operations['sync'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['sync'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['sync'][$docHash]['ops'][$this->getCollectionName()], OP_VIEWS);
                    }
                }

                if(in_array($type, $tableTypes)) {
                    if($asyncConfig[OP_TABLES] == true) {
                        if(!array_key_exists($docHash, $operations['async'])){
                            $operations['async'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['async'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['async'][$docHash]['ops'][$this->getCollectionName()], OP_TABLES);
                    } else{
                        if(!array_key_exists($docHash, $operations['sync'])){
                            $operations['sync'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['sync'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['sync'][$docHash]['ops'][$this->getCollectionName()], OP_TABLES);
                    }
                }

                if(in_array($type, $searchTypes)) {
                    if($asyncConfig[OP_SEARCH] == true) {
                        if(!array_key_exists($docHash, $operations['async'])){
                            $operations['async'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['async'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['async'][$docHash]['ops'][$this->getCollectionName()], OP_SEARCH);
                    } else{
                        if(!array_key_exists($docHash, $operations['sync'])){
                            $operations['sync'][$docHash] = array('id'=>$doc['_id'], 'ops'=>array());
                            $operations['sync'][$docHash]['ops'][$this->getCollectionName()] = array();
                        }
                        array_push($operations['sync'][$docHash]['ops'][$this->getCollectionName()], OP_SEARCH);
                    }
                }
            }
        }
        return $operations;
    }


    /**
     * Submits search params to configured search provider
     * the params array must contain the following keys
     *  -q          the query string to search for
     *  -type       the search document type to restrict results to, in other words _id.type
     *  -indices    an array of indices (from spec) to match query terms against, must specify at least one
     *  -fields     an array of the fields (from spec) you want included in the search results, must specify at least one
     *  -limit      integer the number of results to return per page
     *  -offset     the offset to skip to when returning results
     *
     * this method looks for the above keys in the params array and naively passes them to the search provider which will
     * throw TripodSearchException if any of the params are invalid
     *
     * @param Array $params
     * @throws TripodException - if search provider cannot be found
     * @throws TripodSearchException - if something goes wrong
     * @return Array results
     */
    public function search(Array $params)
    {
        $q          = $params['q'];
        $type       = $params['type'];
        $limit      = $params['limit'];
        $offset     = $params['offset'];
        $indices    = $params['indices'];
        $fields     = $params['fields'];

        $provider = $this->config->searchProvider;

        if(class_exists($provider)){
            $timer = new Timer();
            $timer->start();
            /** @var $searchProvider ITripodSearchProvider */
            $searchProvider = new $provider($this);
            $results =  $searchProvider->search($q, $type, $indices, $fields, $limit, $offset);
            $timer->stop();

            $this->timingLog('SEARCH', array('duration'=>$timer->result(), 'query'=>$params));
            $this->getStat()->timer('SEARCH',$timer->result());
            return $results;
        } else {
            throw new TripodException("Unknown Search Provider: $provider");
        }
    }

    /**
     * Ensure that the graph we want to persist has data with valid cardinality.
     *
     * @param ExtendedGraph $graph
     * @throws TripodCardinalityException
     */
    protected function validateGraphCardinality(ExtendedGraph $graph)
    {
        $cardinality = $this->config->getCardinality($this->dbName, $this->collectionName);
        $namespaces = $this->config->ns;
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

            $this->getTransactionLog()->createNewTransaction($transaction_id, $csDoc['value'][_GRAPHS], $originalCBDs, $this->dbName, $this->collectionName);
            if(empty($originalCBDs)) // didn't get lock on documents
            {
                $this->getTransactionLog()->failTransaction($transaction_id, new Exception('Did not obtain locks on documents'));
                throw new Exception('Did not obtain locks on documents');
            }

            $changes = $this->applyChangeSet($cs,$originalCBDs,$contextAlias, $transaction_id);

            $this->debugLog(MONGO_LOCK,
                array(
                    'description'=>'MongoTripod::storeChanges - Unlocking documents, apply change-set completed',
                    'transaction_id'=>$transaction_id,
                )
            );

            $this->unlockAllDocuments($transaction_id);
            $this->getTransactionLog()->completeTransaction($transaction_id, $changes['newCBDs']);

            $t->stop();
            $this->timingLog(MONGO_WRITE, array('duration'=>$t->result(), 'subjectsOfChange'=>implode(", ",$subjectsOfChange)));
            $this->getStat()->timer(MONGO_WRITE.".{$this->collectionName}",$t->result());

            return $changes;
        }
        catch(Exception $e)
        {
            $this->getStat()->increment(MONGO_ROLLBACK);
            $this->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'Save Failed Rolling back transaction:' . $e->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'subjectsOfChange'=>implode(",",$subjectsOfChange),
                    'mongoDriverError' => $this->db->lastError()
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
        $this->getTransactionLog()->cancelTransaction($transaction_id, $exception);

        if (!empty($originalCBDs)) {  // restore the original CBDs
            foreach ($originalCBDs as $g)
            {
                $result = $this->updateCollection(array('_id' => $g['_id']), $g, array('w' => 1));
                if($result['err']!=NULL )
                {
                    // Error log here
                    $this->errorLog(MONGO_ROLLBACK,
                        array(
                            'description' => 'MongoTripod::rollbackTransaction - Error updating transaction',
                            'exception_message' => $exception->getMessage(),
                            'transaction_id' => $transaction_id,
                            'mongoDriverError' => $this->db->lastError()
                        )
                    );
                    throw new Exception("Failed to restore Original CBDS for transaction: {$transaction_id} stopped at ".$g['_id']);
                }
            }
        }
        else
        {
            $this->errorLog(MONGO_ROLLBACK,
                array(
                    'description'=>'MongoTripod::rollbackTransaction - Unlocking documents',
                    'exception_message' => $exception->getMessage(),
                    'transaction_id'=>$transaction_id,
                    'mongoDriverError' => $this->db->lastError()
                )
            );
        }
        $this->unlockAllDocuments($transaction_id);

        // set transaction to failed
        $this->getTransactionLog()->failTransaction($transaction_id);
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
            $this->errorLog(MONGO_LOCK,
                array(
                    'description'=>'MongoTripod::unlockAllDocuments - Failed to unlock documents (transaction_id - ' .$transaction_id .')',
                    'mongoDriverError' => $this->db->lastError(),
                    $res
                )
            );
            throw new Exception("Failed to unlock documents as part of transaction : ".$transaction_id);
        }
        return true;
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
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'MongoTripod::lockAllDocuments - Attempting to get lock',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'attempt' => $retry
                    )
                );

                $document = $this->lockSingleDocument($s, $transaction_id, $contextAlias);
                if(!empty($document)){

                    $this->debugLog(MONGO_LOCK,
                        array(
                            'description'=>'MongoTripod::lockAllDocuments - Got the lock',
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

            if(count($subjectsOfChange) == count($lockedSubjects)){ //if all subjects of change locked, we are good.
                return $originalCBDs;
            }else{

                if(count($lockedSubjects)) //If any subject was locked, unlock it
                    $this->unlockAllDocuments($transaction_id);

                $this->debugLog(MONGO_LOCK,
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

        $this->errorLog(MONGO_LOCK,
            array(
                'description'=>'Unable to lock all required documents. Exhausted retries',
                'retries' => $this->retriesToGetLock,
                'transaction_id'=>$transaction_id,
                'subjectsOfChange'=>implode(", ",$subjectsOfChange),
                'mongoDriverError' => $this->db->lastError()
            )
        );
        return NULL;
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
                $this->debugLog(MONGO_LOCK,
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
            $document  = $this->collection->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
            if(empty($document)){ //if document is not there, create it
                try{
                    $result = $this->collection->insert(
                        array(
                            '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)
                        ),
                        array("w" => 1)
                    );

                    if(!$result["ok"] || $result['err']!=NULL){
                        throw new Exception("Failed to create new document with error message- " . $result['err']);
                    }
                    $document  = $this->collection->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
                }
                catch(Exception $e){
                    $this->errorLog(MONGO_LOCK,
                        array(
                            'description'=>'MongoTripod::lockSingleDocument - failed when creating new document',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'exception-message' => $e->getMessage(),
                            'mongoDriverError' => $this->db->lastError()
                        )
                    );
                    return false;
                }
            }
            return $document;
        }
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
        if (preg_match('/^CBD_/',$this->collection->getName()))
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
                        $this->getLogger()->error("Removal value {$subjectOfChange} {$predicate} {$object[0]['value']} does not appear in target document to be updated",array("targetGraph"=>$targetGraph->to_ntriples()));
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
                            "findAndModify" => $this->collection->getName(),
                            "query" => $update['criteria'],
                            "update" => $update['change'],
                            "upsert" => $upsert,
                            "new"=>true
                );

                try{
                    $result = $this->db->command($command);
                } catch (Exception $e) {

                    $this->errorLog(MONGO_WRITE,
                        array(
                            'description'=>'Error with Mongo DB command:' . $e->getMessage(),
                            'transaction_id'=>$transaction_id,
                            'mongoDriverError' => $this->db->lastError()
                        )
                    );
                    throw new Exception($e);
                }

                if (!$result["ok"])
                {
                    $this->errorLog("Update failed with err.", $result);
                    throw new Exception("Update failed with err {$result['err']}");
                }

                if($result['value']==null)
                {
                    $this->errorLog(MONGO_WRITE,
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
                            "findAndModify" => $this->collection->getName(),
                            "query" => $delete['criteria'],
                            "update" => $delete['change'],
                            "upsert" => false,
                            "new"=>false
                );

                $result = $this->db->command($command);

                if (!$result["ok"])
                {
                    $this->errorLog("Delete failed with err.", $result);
                    throw new Exception("Delete failed with err {$result['err']}");
                }

                if($result['value']==null)
                {
                    $this->errorLog("Delete failed we did not find a matching document.", $result);
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
     * Returns a count according to the $query and $groupBy conditions
     * @param array $query Mongo query object
     * @param null $groupBy
     * @param null $ttl acceptable time to live if you're willing to accept a cached version of this request
     * @return array|int
     */
    public function getCount($query,$groupBy=null,$ttl=null)
    {
        $t = new Timer();
        $t->start();

        $id = null;
        $results = null;
        if (!empty($ttl))
        {
            $id['query'] = $query;
            $id['groupBy'] = $groupBy;
            $this->debugLog("Looking in cache",array("id"=>$id));
            $candidate = $this->db->selectCollection(TTL_CACHE_COLLECTION)->findOne(array("_id"=>$id));
            if (!empty($candidate))
            {
                $this->debugLog("Found candidate",array("candidate"=>$candidate));
                $ttlTo = new MongoDate($candidate['created']->sec+$ttl);
                if ($ttlTo>(new MongoDate()))
                {
                    // cache hit!
                    $this->debugLog("Cache hit",array("id"=>$id));
                    $results = $candidate['results'];
                }
                else
                {
                    // cache miss
                    $this->debugLog("Cache miss",array("id"=>$id));
                }
            }
        }
        if (empty($results))
        {
            if ($groupBy)
            {
                // todo: if sharded, believe this actually needs to be a MR-function
                $results = $this->collection->group(
                    $groupBy,
                    array("count"=>0),
                    new MongoCode("function(obj,prev) { prev.count++; }"),
                    $query);
            }
            else
            {
                $results = $this->collection->count($query);
            }

            if (!empty($ttl))
            {
                // add to cache
                $cachedResults = array();
                $cachedResults['_id'] = $id;
                $cachedResults['results'] = $results;
                $cachedResults['created'] = new MongoDate();
                $this->debugLog("Adding result to cache",$cachedResults);
                $this->db->selectCollection(TTL_CACHE_COLLECTION)->insert($cachedResults);
            }
        }

        $t->stop();
        $op = ($groupBy) ? MONGO_GROUP : MONGO_COUNT;
        $this->timingLog($op, array('duration'=>$t->result(), 'query'=>$query));
        $this->getStat()->timer("$op.{$this->collectionName}",$t->result());

        return $results;
    }

    /**
     * Selects $fields from the result set determined by $query.
     * Returns an array of all results, each array element is a CBD graph, keyed by r
     * @param array $query
     * @param array $fields array of fields, in the same format as prescribed by MongoPHP
     * @param null $sortBy
     * @param null $limit
     * @param int $offset
     * @param null $context
     * @return array MongoGraphs, keyed by subject
     */
    public function select($query,$fields,$sortBy=null,$limit=null,$offset=0,$context=null)
    {
        $t = new Timer();
        $t->start();

        $contextAlias = $this->getContextAlias($context);

        // make sure context is represented - but not at the expense of $ operands queries failing
        if (array_key_exists('_id',$query) && is_array($query["_id"]))
        {
            if (!array_key_exists(_ID_CONTEXT,$query['_id']) && array_key_exists(_ID_RESOURCE,$query['_id']))
            {
                // add context
                $query["_id"][_ID_CONTEXT] = $contextAlias;
            }
            else
            {
                // check query does not have a $ operand
                foreach ($query["_id"] as $key=>$queryProps)
                {
                    if (substr($key,0,1)=='$' && is_array($queryProps))
                    {
                        foreach ($queryProps as $index=>$queryProp)
                        {
                            if (is_array($queryProp) && array_key_exists(_ID_RESOURCE,$queryProp))
                            {
                                $queryProp[_ID_CONTEXT] = $contextAlias;
                                $query["_id"][$key][$index] = $queryProp;
                            }
                        }
                    }
                }
            }
        }
        else if (!array_key_exists('_id',$query))
        {
            // this query did not have _id referenced at all - just add an _id.c clause
            $query["_id."._ID_CONTEXT] = $contextAlias;
        }

        if (isset($sortBy))
        {
            $results = (empty($limit)) ? $this->collection->find($query,$fields) : $this->collection->find($query,$fields)->skip($offset)->limit($limit);
            $results->sort($sortBy);
        }
        else
        {
            $results = (empty($limit)) ? $this->collection->find($query,$fields) : $this->collection->find($query,$fields)->skip($offset)->limit($limit);
        }

        $t->stop();
        $this->timingLog(MONGO_SELECT, array('duration'=>$t->result(), 'query'=>$query));
        $this->getStat()->timer(MONGO_SELECT.".{$this->collectionName}",$t->result());

        $rows = array();
        $count=$results->count();
        foreach ($results as $doc)
        {
            $row = array();
            foreach ($doc as $key=>$value)
            {
                if ($key == "_id")
                {
                    $row[$key] = $value;
                }
                else
                {
                    if (array_key_exists(VALUE_LITERAL,$value))
                    {
                        $row[$key] = $value[VALUE_LITERAL];
                    }
                    else if (array_key_exists(VALUE_URI,$value))
                    {
                        $row[$key] = $value[VALUE_URI];
                    }
                    else
                    {
                        $row[$key] = array();
                        // possible array of values
                        foreach ($value as $v)
                        {
                            $row[$key][] = array_key_exists(VALUE_LITERAL,$v) ? $v[VALUE_LITERAL] : $v[VALUE_URI];
                        }
                    }
                }
            }
            $rows[] = $row;
        }

        $result = array(
            "head"=>array(
                "count"=>$count,
                "offset"=>$offset,
                "limit"=>$limit
            ),
            "results"=>$rows);
        return $result;
    }

    /**
     * Returns a graph as the result of $query. Useful replacement for DESCRIBE ... WHERE
     * @deprecated use getGraph
     * @param $query array
     * @return MongoGraph
     */
    public function describe($query)
    {
        return $this->fetchGraph($query,MONGO_DESCRIBE_WITH_CONDITION);
    }

    /**
     * Returns a graph of data matching $query. Only triples with properties mapping to those in $includeProperties will
     * be added. If $includeProperties is empty, all properties will be included. If data matches $query, but does not
     * contain properties specified in $includeProperties, an empty graph will be returned
     * todo: unit test
     * @param $query
     * @param array $includeProperties
     * @return MongoGraph
     */
    public function graph($query, $includeProperties=array())
    {
        return $this->fetchGraph($query,MONGO_GET_GRAPH,null,$includeProperties);
    }

    /**
     * Retuns the eTag of the $resource, useful for cache control or optimistic concurrency control
     * @param $resource
     * @param null $context
     * @return string
     */
    public function getETag($resource,$context=null)
    {
        $this->getStat()->increment(MONGO_GET_ETAG);
        $resource = $this->labeller->uri_to_alias($resource);
        $query = array(
            "_id" => array(
                _ID_RESOURCE=>$resource,
                _ID_CONTEXT=>$this->getContextAlias($context)));
        $doc = $this->collection->findOne($query,array(_UPDATED_TS=>true));
        /* @var $lastUpdatedDate MongoDate */
        $lastUpdatedDate = ($doc!=null && array_key_exists(_UPDATED_TS,$doc)) ? $doc[_UPDATED_TS] : null;
        return ($lastUpdatedDate==null) ? '' : $lastUpdatedDate->__toString();
    }


    // getters and setters for the delegates

    /**
     * @return MongoTransactionLog
     */
    public function getTransactionLog()
    {
        if($this->transaction_log==null)
        {
            $this->transaction_log = new MongoTransactionLog();
        }
        return $this->transaction_log;
    }

    /**
     * @return MongoTripodViews
     */
    public function getTripodViews()
    {
        if($this->tripod_views==null)
        {
            $this->tripod_views = new MongoTripodViews(
                $this->db,
                $this->collection,
                $this->defaultContext,
                $this->stat
            );
        }
        return $this->tripod_views;
    }

    /**
     * @return MongoTripodTables
     */
    public function getTripodTables()
    {
        if ($this->tripod_tables==null)
        {
            $this->tripod_tables = new MongoTripodTables(
                $this->db,
                $this->collection,
                $this->defaultContext,
                $this->stat
            );
        }
        return $this->tripod_tables;
    }

    /**
     * @return MongoTripodSearchIndexer
     */
    public function getSearchIndexer()
    {
        if ($this->search_indexer==null)
        {
            $this->search_indexer = new MongoTripodSearchIndexer($this);
        }
        return $this->search_indexer;
    }

    /**
     * @param MongoTransactionLog $transactionLog
     */
    public function setTransactionLog(MongoTransactionLog $transactionLog)
    {
        $this->transaction_log = $transactionLog;
    }

    /**
     * @return string
     */
    protected function getUniqId()
    {
        return uniqid('', true);
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
                if($data['collection'] == $this->getCollectionName()){
                    $observer = $this->getObserver($op);
                } else {
                    $observer =$this->getMongoTripod($data)->getObserver($op);
                }
                $subject->attach($observer);
            }
            $t = new Timer();
            $t->start();

            $subject->notify();

            $t->stop();
            $this->timingLog(MONGO_ON_THE_FLY_MR,array(
                "duration"=>$t->result(),
                "operations"=>var_export($data['operations'],true),
                "database"=>$data['database'],
                "collection"=>$data['collection'],
                "resource"=>$data['r']
            ));
            $this->getStat()->timer(MONGO_ON_THE_FLY_MR,$t->result());
        }
    }

    /////////////////////////// QUEUE RELATED METHODS BELOW HERE ///////////////////////////////////////


    protected function queueASyncOperations(Array $modifiedSubjects)
    {
        foreach ($modifiedSubjects as $subject)
        {
            /* @var $subject ModifiedSubject */
            $data = $subject->getData();
            $this->debugLog(MONGO_ADD_TO_QUEUE,array(
                    "operations"=>var_export($data['operations'],true),
                    "database"=>$data['database'],
                    "collection"=>$data['collection'],
                    "resource"=>$data['r']
                )
            );
            $this->getStat()->increment(MONGO_ADD_TO_QUEUE);
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

    /**
     * @param $data
     * @return MongoTripod
     */
    protected function getMongoTripod($data) {
        return new MongoTripod(
            $data['collection'],
            $data['database'],
            array('stat'=>$this->stat));
    }



    /**
     * Returns the correct observer as per the supported operation
     * @param $operation string must be either views, tables or search
     * @return SplObserver
     * @throws TripodException when an unsupported operation is requested
     */
    public function getObserver($operation)
    {
        switch ($operation)
        {
            case OP_VIEWS:
                return $this->getTripodViews();
            case OP_TABLES:
                return $this->getTripodTables();
            case OP_SEARCH:
                return $this->getSearchIndexer();
            default:
                throw new TripodException("Undefined operation '$operation' requested");
        }
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

        $cursor = $this->getTransactionLog()->getCompletedTransactions($this->dbName, $this->collectionName, $fromDate, $toDate);
        while($cursor->hasNext()) {
            $result = $cursor->getNext();
            $this->applyTransaction($result);
        }

        return true;
    }

    public function applyTransaction($transaction)
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

    /**
     * This proxy method allows us to mock updates against $this->collection
     * @param $query
     * @param $update
     * @param $options
     * @return bool
     */
    protected function updateCollection($query, $update, $options)
    {
        return $this->collection->update($query, $update, $options);
    }


    /**
     * @return string
     */
    protected function generateTransactionId()
    {
        return 'transaction_' . $this->getUniqId();
    }

    /**
     * @return MongoCollection
     */
    protected function getAuditManualRollbacksCollection()
    {
        return $this->db->selectCollection(AUDIT_MANUAL_ROLLBACKS_COLLECTION);
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

    /**
     * For mocking
     * @return MongoTripodConfig
     */
    protected function getMongoTripodConfigInstance()
    {
        return MongoTripodConfig::getInstance();
    }

    /**
     * For mocking
     * @return MongoTripodLabeller
     */
    protected function getLabeller()
    {
        return new MongoTripodLabeller();
    }

}
