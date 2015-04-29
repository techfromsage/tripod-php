<?php


/** @noinspection PhpIncludeInspection */

$TOTAL_TIME=0;

class MongoTripod extends MongoTripodBase implements ITripod
{

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
    private $originalCollectionReadPreference = array();

    /**
     * @var array The original read preference gets stored here
     *            when changing for a write.
     */
    private $originalDbReadPreference = array();

    /**
     * @var array
     */
    private $async = null;

    /**
     * @var Integer
     */
    private $retriesToGetLock;

    /**
     * @var MongoTripodUpdates
     */
    private $dataUpdater;

    /**
     * Constructor for MongoTripod
     *
     * @param string $podName
     * @param string $storeName
     * @param array $opts an Array of options: <ul>
     * <li>defaultContext: (string) to use where a specific default context is not defined. Default is Null</li>
     * <li>async: (array) determines the async behaviour of views, tables and search. For each of these array keys, if set to true, generation of these elements will be done asyncronously on save. Default is array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true)</li>
     * <li>stat: this sets the stats object to use to record statistics around operations performed by Tripod. Default is null</li>
     * <li>readPreference: The Read preference to set for Mongo: Default is Mongo:RP_PRIMARY_PREFERRED</li>
     * <li>retriesToGetLock: Retries to do when unable to get lock on a document, default is 20</li></ul>
     */
    public function __construct($podName, $storeName, $opts=array())
    {
        $opts = array_merge(array(
                'defaultContext'=>null,
                OP_ASYNC=>array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true),
                'stat'=>null,
                'readPreference'=>MongoClient::RP_PRIMARY_PREFERRED,
                'retriesToGetLock' => 20)
            ,$opts);
        $this->podName = $podName;
        $this->storeName = $storeName;
        $this->config = $this->getMongoTripodConfigInstance();

        $this->labeller = $this->getLabeller();

        // default context
        $this->defaultContext = $opts['defaultContext'];

        //max retries to get lock
        $this->retriesToGetLock = $opts['retriesToGetLock'];

        $this->collection = $this->config->getCollectionForCBD($storeName, $podName, $opts['readPreference']);

        // fill in and default any missing keys for $async array. Default is views are sync, tables and search async
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
        $this->getTripodTables()->generateTableRows($tableType,$resource,$context);
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
        return $this->getDataUpdater()->saveChanges($oldGraph, $newGraph, $context, $description);
    }

    /**
     * Get locked documents for a date range or all documents if no date range is given
     * @param $fromDateTime String
     * @param $tillDateTime String
     * @return array
     */
    public function getLockedDocuments($fromDateTime = null , $tillDateTime = null)
    {
        return $this->getDataUpdater()->getLockedDocuments($fromDateTime, $tillDateTime);
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
        return $this->getDataUpdater()->removeInertLocks($transaction_id, $reason);
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

        $provider = $this->config->getSearchProviderClassName($this->storeName);

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
            $candidate = $this->config->getCollectionForTTLCache($this->storeName)->findOne(array("_id"=>$id));
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
                $this->config->getCollectionForTTLCache($this->storeName)->insert($cachedResults);
            }
        }

        $t->stop();
        $op = ($groupBy) ? MONGO_GROUP : MONGO_COUNT;
        $this->timingLog($op, array('duration'=>$t->result(), 'query'=>$query));
        $this->getStat()->timer("$op.{$this->podName}",$t->result());

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
        $this->getStat()->timer(MONGO_SELECT.".{$this->podName}",$t->result());

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

    /**
     * @return MongoTripodViews
     */
    public function getTripodViews()
    {
        if($this->tripod_views==null)
        {
            $this->tripod_views = new MongoTripodViews(
                $this->storeName,
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
                $this->storeName,
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
        $this->getDataUpdater()->setTransactionLog($transactionLog);
    }

    /**
     * replays all transactions from the transaction log, use the function params to control the from and to date if you
     * only want to replay transactions created during specific window
     * @param null $fromDate
     * @param null $toDate
     * @return bool
     */
    public function replayTransactionLog($fromDate = null, $toDate = null)
    {
        return $this->getDataUpdater()->replayTransactionLog($fromDate, $toDate);
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
     * Returns the composite that can perform the supported operation
     * @param $operation string must be either OP_VIEWS, OP_TABLES or OP_SEARCH
     * @return IComposite
     * @throws TripodException when an unsupported operation is requested
     */
    public function getComposite($operation)
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

    /**
     * For mocking
     * @return MongoTripodLabeller
     */
    protected function getLabeller()
    {
        return new MongoTripodLabeller();
    }

    /**
     * Returns the delegate object for saving data in Mongo
     *
     * @return MongoTripodUpdates
     */
    protected function getDataUpdater()
    {
        if(!isset($this->dataUpdater))
        {
            $readPreference = $this->collection->getReadPreference();
            $opts = array(
                'defaultContext'=>$this->defaultContext,
                OP_ASYNC=>$this->async,
                'stat'=>$this->stat,
                'readPreference'=>$readPreference['type'],
                'retriesToGetLock' => $this->retriesToGetLock
            );

            $this->dataUpdater = new MongoTripodUpdates($this, $opts);
        }
        return $this->dataUpdater;
    }

}
