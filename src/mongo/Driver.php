<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\BSON\UTCDateTime;
use MongoDB\Driver\ReadPreference;
use Tripod\Exceptions\Exception;
use Tripod\Exceptions\SearchException;
use Tripod\ExtendedGraph;
use Tripod\IDriver;
use Tripod\IEventHook;
use Tripod\ISearchProvider;
use Tripod\Mongo\Composites\SearchIndexer;
use Tripod\Mongo\Composites\Tables;
use Tripod\Mongo\Composites\Views;
use Tripod\Timer;

class Driver extends DriverBase implements IDriver
{
    private ?Views $tripod_views = null;

    private ?Tables $tripod_tables = null;

    private ?SearchIndexer $searchIndexer = null;

    /**
     * @var array<string, bool> keyed by OP_VIEWS/OP_TABLES/OP_SEARCH; OP_SEARCH is absent when no search provider is configured
     */
    private array $async;

    private int $retriesToGetLock;

    private ?Updates $updates = null;

    /**
     * Constructor for Driver.
     *
     * @param array<string, mixed> $opts an Array of options: <ul>
     *                                   <li>defaultContext: (string) to use where a specific default context is not defined. Default is Null</li>
     *                                   <li>async: (array) determines the async behaviour of views, tables and search. For each of these array keys, if set to true, generation of these elements will be done asyncronously on save. Default is array(OP_VIEWS=>false,OP_TABLES=>true,OP_SEARCH=>true)</li>
     *                                   <li>stat: this sets the stats object to use to record statistics around operations performed by Driver. Default is null</li>
     *                                   <li>readPreference: The Read preference to set for Mongo: Default is ReadPreference::PRIMARY_PREFERRED</li>
     *                                   <li>retriesToGetLock: Retries to do when unable to get lock on a document, default is 20</li></ul>
     */
    public function __construct(string $podName, string $storeName, array $opts = [])
    {
        $opts = array_merge([
            'defaultContext' => null,
            OP_ASYNC => [OP_VIEWS => false, OP_TABLES => true, OP_SEARCH => true],
            'statsConfig' => [],
            'readPreference' => ReadPreference::PRIMARY_PREFERRED,
            'retriesToGetLock' => 20,
        ], $opts);

        $this->podName = $podName;
        $this->storeName = $storeName;
        $this->config = $this->getConfigInstance();

        $this->labeller = $this->getLabeller();

        // default context
        $this->defaultContext = $opts['defaultContext'];

        // max retries to get lock
        $this->retriesToGetLock = $opts['retriesToGetLock'];

        $this->collection = $this->config->getCollectionForCBD($storeName, $podName, $opts['readPreference']);

        // fill in and default any missing keys for $async array. Default is views are sync, tables and search async
        $async = $opts[OP_ASYNC];
        if (!array_key_exists(OP_VIEWS, $async)) {
            $async[OP_VIEWS] = false;
        }

        if (!array_key_exists(OP_TABLES, $async)) {
            $async[OP_TABLES] = true;
        }

        if (!array_key_exists(OP_SEARCH, $async)) {
            $async[OP_SEARCH] = true;
        }

        // if there is no es configured then remove OP_SEARCH from async (no point putting these onto the queue) TRI-19
        if ($this->config->getSearchDocumentSpecifications($this->storeName) == null) {
            unset($async[OP_SEARCH]);
        }

        $this->async = $async;

        if (isset($opts['stat'])) {
            $this->statsConfig = $opts['stat']->getConfig();
            $this->setStat($opts['stat']);
        } else {
            $this->statsConfig = $opts['statsConfig'];
        }

        // Set the read preference if passed in
        if ($opts['readPreference']) {
            $this->readPreference = $opts['readPreference'];
        }
    }

    /**
     * Pass a subject to $resource and have mongo return a DESCRIBE <?resource>.
     *
     * @param string      $resource uri resource you'd like to describe
     * @param string|null $context  string uri of the context, or named graph, you'd like to describe from
     *
     * @return MongoGraph
     */
    public function describeResource(string $resource, ?string $context = null): ExtendedGraph
    {
        $resource = $this->labeller->uri_to_alias($resource);
        $query = [
            _ID_KEY => [
                _ID_RESOURCE => $resource,
                _ID_CONTEXT => $this->getContextAlias($context),
            ],
        ];

        return $this->fetchGraph($query, MONGO_DESCRIBE);
    }

    /**
     * Pass subjects as to $resources and have mongo return a DESCRIBE <?resource[0]> <?resource[1]> <?resource[2]> etc.
     *
     * @return MongoGraph
     */
    public function describeResources(array $resources, ?string $context = null): ExtendedGraph
    {
        $ids = [];
        foreach ($resources as $resource) {
            $resource = $this->labeller->uri_to_alias($resource);
            $ids[] = [
                _ID_RESOURCE => $resource,
                _ID_CONTEXT => $this->getContextAlias($context),
            ];
        }

        $query = [_ID_KEY => ['$in' => $ids]];

        return $this->fetchGraph($query, MONGO_MULTIDESCRIBE);
    }

    /**
     * @return MongoGraph
     */
    public function getViewForResource(?string $resource, string $viewType): ExtendedGraph
    {
        return $this->getTripodViews()->getViewForResource($resource, $viewType);
    }

    /**
     * @param string[] $resources
     *
     * @return MongoGraph
     */
    public function getViewForResources(array $resources, string $viewType): ExtendedGraph
    {
        return $this->getTripodViews()->getViewForResources($resources, $viewType);
    }

    /**
     * @return MongoGraph
     */
    public function getViews(array $filter, string $viewType): ExtendedGraph
    {
        return $this->getTripodViews()->getViews($filter, $viewType);
    }

    public function getTableRows(
        string $tableType,
        array $filter = [],
        ?array $sortBy = [],
        ?int $offset = 0,
        ?int $limit = 10,
        array $options = []
    ): array {
        return $this->getTripodTables()->getTableRows(
            $tableType,
            $filter,
            $sortBy,
            $offset,
            $limit,
            $options
        );
    }

    public function generateTableRows(string $tableType, ?string $resource = null, ?string $context = null): void
    {
        $this->getTripodTables()->generateTableRows($tableType, $resource, $context);
    }

    public function getDistinctTableColumnValues(string $tableType, string $fieldName, array $filter = []): array
    {
        return $this->getTripodTables()->distinct($tableType, $fieldName, $filter);
    }

    /**
     * Create and apply a changeset which is the delta between $oldGraph and $newGraph.
     *
     * @throws Exception
     */
    public function saveChanges(
        ExtendedGraph $oldGraph,
        ExtendedGraph $newGraph,
        ?string $context = null,
        ?string $description = null
    ): bool {
        return $this->getDataUpdater()->saveChanges($oldGraph, $newGraph, $context, $description);
    }

    /**
     * Get locked documents for a date range or all documents if no date range is given.
     */
    public function getLockedDocuments(?string $fromDateTime = null, ?string $tillDateTime = null): array
    {
        return $this->getDataUpdater()->getLockedDocuments($fromDateTime, $tillDateTime);
    }

    /**
     * Remove locks that are there forever, creates a audit entry to keep track who and why removed these locks.
     *
     * @throws \Exception If something goes wrong when unlocking documents, or creating audit entries
     */
    public function removeInertLocks(string $transaction_id, string $reason): bool
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
     *  -offset     the offset to skip to when returning results.
     *
     * this method looks for the above keys in the params array and naively passes them to the search provider which will
     * throw SearchException if any of the params are invalid
     *
     * @return array results
     *
     * @throws Exception       - if search provider cannot be found
     * @throws SearchException - if something goes wrong
     */
    public function search(array $params): array
    {
        $q = $params['q'];
        $type = $params['type'];
        $limit = $params['limit'];
        $offset = $params['offset'];
        $indices = $params['indices'];
        $fields = $params['fields'];

        $provider = $this->config->getSearchProviderClassName($this->storeName);

        if (class_exists($provider)) {
            $timer = new Timer();
            $timer->start();

            /** @var ISearchProvider $searchProvider */
            $searchProvider = new $provider($this);
            $results = $searchProvider->search($q, $type, $indices, $fields, $limit, $offset);
            $timer->stop();

            $this->timingLog('SEARCH', ['duration' => $timer->result(), 'query' => $params]);
            $this->getStat()->timer('SEARCH', $timer->result());

            return $results;
        }

        throw new Exception('Unknown Search Provider: ' . $provider);
    }

    /**
     * Returns a count according to the $query and $groupBy conditions.
     *
     * @param array    $query Mongo query object
     * @param int|null $ttl   acceptable time to live if you're willing to accept a cached version of this request
     *
     * @return ($groupBy is null ? int : array<int|string, int>) counts grouped by the $groupBy field, or a total count
     */
    public function getCount(array $query, ?string $groupBy = null, ?int $ttl = null)
    {
        $t = new Timer();
        $t->start();

        $id = null;
        $results = null;
        if (!empty($ttl)) {
            $id['query'] = $query;
            $id['groupBy'] = $groupBy;
            $this->debugLog('Looking in cache', ['id' => $id]);
            $candidate = $this->config->getCollectionForTTLCache($this->storeName)->findOne([_ID_KEY => $id]);
            if (!empty($candidate)) {
                $this->debugLog('Found candidate', ['candidate' => $candidate]);

                $ttlTo = DateUtil::getMongoDate((((int) $candidate['created']->__toString() / 1000) + $ttl) * 1000);
                if ($ttlTo > DateUtil::getMongoDate()) {
                    // cache hit!
                    $this->debugLog('Cache hit', ['id' => $id]);
                    $results = $candidate['results'];
                } else {
                    // cache miss
                    $this->debugLog('Cache miss', ['id' => $id]);
                }
            }
        }

        if (empty($results)) {
            if ($groupBy) {
                $ops = [
                    ['$match' => $query],
                    ['$group' => [_ID_KEY => '$' . $groupBy, 'total' => ['$sum' => 1]]],
                ];
                $cursor = $this->collection->aggregate($ops);
                $results = [];
                foreach ($cursor as $doc) {
                    if (!is_array($doc[_ID_KEY])) {
                        $results[$doc[_ID_KEY] ?? ''] = $doc['total'];
                    } else {
                        $results[implode(';', $doc[_ID_KEY])] = $doc['total'];
                    }
                }
            } else {
                $results = $this->collection->count($query);
            }

            if (!empty($ttl)) {
                // add to cache
                $cachedResults = [];
                $cachedResults[_ID_KEY] = $id;
                $cachedResults['results'] = $results;
                $cachedResults['created'] = DateUtil::getMongoDate();
                $this->debugLog('Adding result to cache', $cachedResults);
                $result = $this->config->getCollectionForTTLCache($this->storeName)->insertOne($cachedResults);
                if (!$result->isAcknowledged()) {
                    $this->debugLog('Insert cache result not acknowledged');
                }
            }
        }

        $t->stop();
        $op = ($groupBy) ? MONGO_GROUP : MONGO_COUNT;
        $this->timingLog($op, ['duration' => $t->result(), 'query' => $query]);
        $this->getStat()->timer(sprintf('%s.%s', $op, $this->podName), $t->result());

        return $results;
    }

    /**
     * Selects $fields from the result set determined by $query.
     * Returns an array of all results, each array element is a CBD graph, keyed by r.
     *
     * @param array<string, mixed> $fields array of fields, in the same format as prescribed by MongoPHP
     *
     * @return array<string, array<int|string, int|mixed[]|null>>
     */
    public function select(array $query, array $fields, ?array $sortBy = null, ?int $limit = null, ?int $offset = 0, ?string $context = null): array
    {
        $t = new Timer();
        $t->start();

        $contextAlias = $this->getContextAlias($context);

        // make sure context is represented - but not at the expense of $ operands queries failing
        if (array_key_exists(_ID_KEY, $query) && is_array($query[_ID_KEY])) {
            if (!array_key_exists(_ID_CONTEXT, $query[_ID_KEY]) && array_key_exists(_ID_RESOURCE, $query[_ID_KEY])) {
                // add context
                $query[_ID_KEY][_ID_CONTEXT] = $contextAlias;
            } else {
                // check query does not have a $ operand
                foreach ($query[_ID_KEY] as $key => $queryProps) {
                    if (substr($key, 0, 1) == '$' && is_array($queryProps)) {
                        foreach ($queryProps as $index => $queryProp) {
                            if (is_array($queryProp) && array_key_exists(_ID_RESOURCE, $queryProp)) {
                                $queryProp[_ID_CONTEXT] = $contextAlias;
                                $query[_ID_KEY][$key][$index] = $queryProp;
                            }
                        }
                    }
                }
            }
        } elseif (!array_key_exists(_ID_KEY, $query)) {
            // this query did not have _id referenced at all - just add an _id.c clause
            $query[_ID_KEY . '.' . _ID_CONTEXT] = $contextAlias;
        }

        $findOptions = [
            'projection' => $fields,
        ];
        if (!empty($limit)) {
            $findOptions['skip'] = $offset ?? 0;
            $findOptions['limit'] = $limit;
        }

        if (isset($sortBy)) {
            $findOptions['sort'] = $sortBy;
        }

        $results = $this->collection->find($query, $findOptions);

        $t->stop();
        $this->timingLog(MONGO_SELECT, ['duration' => $t->result(), 'query' => $query]);
        $this->getStat()->timer(MONGO_SELECT . ('.' . $this->podName), $t->result());

        $rows = [];
        $count = $this->collection->count($query);

        foreach ($results as $doc) {
            $row = [];
            foreach ($doc as $key => $value) {
                if ($key == _ID_KEY || $key === _VERSION) {
                    $row[$key] = $value;
                } elseif (is_array($value)) {
                    if (isset($value[VALUE_LITERAL])) {
                        $row[$key] = $value[VALUE_LITERAL];
                    } elseif (isset($value[VALUE_URI])) {
                        $row[$key] = $value[VALUE_URI];
                    } else {
                        $row[$key] = [];
                        // possible array of values
                        foreach ($value as $v) {
                            if (isset($v[VALUE_LITERAL])) {
                                $row[$key][] = $v[VALUE_LITERAL];
                            } elseif (isset($v[VALUE_URI])) {
                                $row[$key][] = $v[VALUE_URI];
                            }
                        }
                    }
                }
            }

            $rows[] = $row;
        }

        return [
            'head' => [
                'count' => $count,
                'offset' => $offset,
                'limit' => $limit,
            ],
            'results' => $rows,
        ];
    }

    /**
     * Returns a graph as the result of $query. Useful replacement for DESCRIBE ... WHERE.
     *
     * @deprecated use getGraph
     *
     * @return MongoGraph
     */
    public function describe(array $query): ExtendedGraph
    {
        return $this->fetchGraph($query, MONGO_DESCRIBE_WITH_CONDITION);
    }

    /**
     * Returns a graph of data matching $query. Only triples with properties mapping to those in $includeProperties will
     * be added. If $includeProperties is empty, all properties will be included. If data matches $query, but does not
     * contain properties specified in $includeProperties, an empty graph will be returned
     * todo: unit test.
     *
     * @param array $filter            conditions to filter by
     * @param array $includeProperties only include these predicates, empty array means return all predicates
     *
     * @return MongoGraph
     */
    public function graph(array $filter, array $includeProperties = []): ExtendedGraph
    {
        return $this->fetchGraph($filter, MONGO_GET_GRAPH, null, $includeProperties);
    }

    /**
     * Returns the eTag of the $resource, useful for cache control or optimistic concurrency control.
     */
    public function getETag(string $resource, ?string $context = null): string
    {
        $this->getStat()->increment(MONGO_GET_ETAG);
        $resource = $this->labeller->uri_to_alias($resource);
        $query = [
            _ID_KEY => [
                _ID_RESOURCE => $resource,
                _ID_CONTEXT => $this->getContextAlias($context),
            ],
        ];
        $doc = $this->collection->findOne($query, ['projection' => [_UPDATED_TS => true]]);

        /** @var UTCDateTime|null $lastUpdatedDate */
        $lastUpdatedDate = $doc[_UPDATED_TS] ?? null;
        if ($lastUpdatedDate === null) {
            return '';
        }

        $milliseconds = (int) $lastUpdatedDate->__toString();
        $seconds = intdiv($milliseconds, 1000);
        $fraction = ($milliseconds % 1000) / 1000;

        return sprintf('%.8f %d', $fraction, $seconds);
    }

    public function getTripodViews(): Views
    {
        if ($this->tripod_views == null) {
            $this->tripod_views = new Views(
                $this->storeName,
                $this->collection,
                $this->defaultContext,
                $this->stat,
                $this->readPreference
            );
        }

        return $this->tripod_views;
    }

    public function getTripodTables(): Tables
    {
        if ($this->tripod_tables == null) {
            $this->tripod_tables = new Tables(
                $this->storeName,
                $this->collection,
                $this->defaultContext,
                $this->stat,
                $this->readPreference
            );
        }

        return $this->tripod_tables;
    }

    public function getSearchIndexer(): SearchIndexer
    {
        if ($this->searchIndexer == null) {
            $this->searchIndexer = new SearchIndexer($this, $this->readPreference);
        }

        return $this->searchIndexer;
    }

    public function setTransactionLog(TransactionLog $transactionLog): void
    {
        $this->getDataUpdater()->setTransactionLog($transactionLog);
    }

    /**
     * replays all transactions from the transaction log, use the function params to control the from and to date if you
     * only want to replay transactions created during specific window.
     *
     * @param string|null $fromDate only transactions after this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     * @param string|null $toDate   only transactions before this specified date. This must be a datetime string i.e. '2010-01-15 00:00:00'
     */
    public function replayTransactionLog(?string $fromDate = null, ?string $toDate = null): bool
    {
        return $this->getDataUpdater()->replayTransactionLog($fromDate, $toDate);
    }

    /**
     * Register an event hook, which will be executed when the event fires.
     *
     * @throws Exception when an unrecognised event type is given
     */
    public function registerHook(string $eventType, IEventHook $hook): void
    {
        switch ($eventType) {
            case IEventHook::EVENT_SAVE_CHANGES:
                $this->getDataUpdater()->registerSaveChangesEventHook($hook);

                break;

            default:
                throw new Exception(sprintf('Unrecognised type %s whilst registering event hook', $eventType));
        }
    }

    /**
     * Returns the composite that can perform the supported operation.
     *
     * @param $operation string must be either OP_VIEWS, OP_TABLES or OP_SEARCH
     *
     * @return SearchIndexer|Tables|Views
     *
     * @throws Exception when an unsupported operation is requested
     */
    public function getComposite(string $operation)
    {
        switch ($operation) {
            case OP_VIEWS:
                return $this->getTripodViews();

            case OP_TABLES:
                return $this->getTripodTables();

            case OP_SEARCH:
                return $this->getSearchIndexer();

            default:
                throw new Exception(sprintf("Undefined operation '%s' requested", $operation));
        }
    }

    /**
     * For mocking.
     */
    protected function getLabeller(): Labeller
    {
        return new Labeller();
    }

    /**
     * Returns the delegate object for saving data in Mongo.
     */
    protected function getDataUpdater(): Updates
    {
        if ($this->updates === null) {
            $readPreference = $this->collection->getReadPreference()->getModeString();

            $opts = [
                'defaultContext' => $this->defaultContext,
                OP_ASYNC => $this->async,
                'stat' => $this->stat,
                'readPreference' => $readPreference,
                'retriesToGetLock' => $this->retriesToGetLock,
                'statsConfig' => $this->statsConfig,
            ];

            $this->updates = new Updates($this, $opts);
        }

        return $this->updates;
    }
}
