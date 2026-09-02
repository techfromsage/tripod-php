<?php

declare(strict_types=1);

namespace Tripod\Mongo\Composites;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';

use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\CursorInterface;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\ReadPreference;
use Tripod\Config;
use Tripod\Exceptions\LabellerException;
use Tripod\ITripodStat;
use Tripod\Mongo\DateUtil;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\ApplyOperation;
use Tripod\Mongo\Labeller;
use Tripod\Timer;

class Tables extends CompositeBase
{
    /**
     * Modifier config - list of allowed functions and their attributes that can be passed through in tablespecs.json
     * Note about the "true" value - this is so that the keys are defined as keys rather than values. If we move to
     * a json schema we could define the types of attribute and whether they are required or not.
     *
     * @static
     */
    public static array $predicateModifiers = [
        'join' => [
            'glue' => true,
            'predicates' => true,
        ],
        'lowercase' => [
            'predicates' => true,
        ],
        'date' => [
            'predicates' => true,
        ],
    ];

    /**
     * Computed field config - A list of valid functions to write dynamic table row field values.
     *
     * @static
     */
    public static array $computedFieldFunctions = ['conditional', 'replace', 'arithmetic'];

    /**
     * Computed conditional config - list of allowed conditional operators.
     *
     * @static
     */
    public static array $conditionalOperators = ['>', '<', '>=', '<=', '==', '!=', 'contains', 'not contains', '~=', '!~'];

    /**
     * Computed arithmetic config - list of allowed arithmetic operators.
     *
     * @static
     */
    public static array $arithmeticOperators = ['+', '-', '*', '/', '%'];

    protected array $temporaryFields = [];

    /**
     * Construct accepts actual objects rather than strings as this class is a delegate of
     * Tripod and should inherit connections set up there.
     */
    public function __construct(
        string $storeName,
        Collection $collection,
        ?string $defaultContext,
        ?ITripodStat $stat = null,
        string $readPreference = ReadPreference::PRIMARY
    ) {
        $this->labeller = new Labeller();
        $this->storeName = $storeName;
        $this->collection = $collection;
        $this->podName = $collection->getCollectionName();
        $this->config = Config::getInstance();
        $this->defaultContext = $this->labeller->uri_to_alias($defaultContext); // make sure default context is qnamed if applicable
        $this->stat = $stat;
        $this->readPreference = $readPreference;
    }

    /**
     * Receive update from subject.
     */
    public function update(ImpactedSubject $subject): void
    {
        $resource = $subject->getResourceId();
        $resourceUri = $resource[_ID_RESOURCE];
        $context = $resource[_ID_CONTEXT];

        $this->generateTableRowsForResource($resourceUri, $context, $subject->getSpecTypes());
    }

    /**
     * Returns an array of the rdf types that will trigger the table specification.
     */
    public function getTypesInSpecifications(): array
    {
        return $this->config->getTypesInTableSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * Returns an array of table rows that are impacted by the changes.
     *
     * @return mixed[]
     */
    public function findImpactedComposites(array $resourcesAndPredicates, string $contextAlias): array
    {
        $contextAlias = $this->getContextAlias($contextAlias); // belt and braces

        $tablePredicates = [];

        foreach ($this->getConfigInstance()->getTableSpecifications($this->storeName) as $tableSpec) {
            if (isset($tableSpec[_ID_KEY])) {
                $tablePredicates[$tableSpec[_ID_KEY]] = $this->getConfigInstance()
                    ->getDefinedPredicatesInSpec($this->storeName, $tableSpec[_ID_KEY]);
            }
        }

        // build a filter - will be used for impactIndex detection and finding direct tables to re-gen
        $tableFilters = [];
        $resourceFilters = [];
        foreach ($resourcesAndPredicates as $resource => $resourcePredicates) {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $id = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];
            // If we don't have a working config or there are no predicates listed, remove all
            // rows associated with the resource in all tables
            if ($tablePredicates === [] || empty($resourcePredicates)) {
                // build $filter for queries to impact index
                $resourceFilters[] = $id;
            } else {
                foreach ($tablePredicates as $tableType => $predicates) {
                    // Only look for table rows if the changed predicates are actually defined in the tablespec
                    if (array_intersect($resourcePredicates, $predicates)) {
                        if (!isset($tableFilters[$tableType])) {
                            $tableFilters[$tableType] = [];
                        }

                        // build $filter for queries to impact index
                        $tableFilters[$tableType][] = $id;
                    }
                }
            }
        }

        if ($tableFilters === [] && $resourceFilters !== []) {
            $query = ['value.' . _IMPACT_INDEX => ['$in' => $resourceFilters]];
        } else {
            $query = [];
            foreach ($tableFilters as $tableType => $filters) {
                // first re-gen table rows where resources appear in the impact index
                $query[] = ['value.' . _IMPACT_INDEX => ['$in' => $filters], _ID_KEY . '.' . _ID_TYPE => $tableType];
            }

            if ($resourceFilters !== []) {
                $query[] = ['value.' . _IMPACT_INDEX => ['$in' => $resourceFilters]];
            }

            if (count($query) === 1) {
                $query = $query[0];
            } elseif (count($query) > 1) {
                $query = ['$or' => $query];
            }
        }

        if ($query === []) {
            return [];
        }

        $affectedTableRows = [];

        foreach ($this->config->getCollectionsForTables($this->storeName) as $collection) {
            $t = new Timer();
            $t->start();
            $tableRows = $collection->find($query, ['projection' => [_ID_KEY => true]]);
            $t->stop();
            $this->timingLog(MONGO_FIND_IMPACTED, ['duration' => $t->result(), 'query' => $query, 'storeName' => $this->storeName, 'collection' => $collection]);
            foreach ($tableRows as $t) {
                $affectedTableRows[] = $t;
            }
        }

        return $affectedTableRows;
    }

    public function getSpecification(string $storeName, string $tableSpecId): ?array
    {
        return $this->config->getTableSpecification($storeName, $tableSpecId);
    }

    /**
     * Returns the operation this composite can satisfy.
     */
    public function getOperationType(): string
    {
        return OP_TABLES;
    }

    /**
     * Query the tables collection and return the results.
     *
     * By default the matching rows are returned as a list of table documents (ArrayObject
     * instances, or plain arrays depending on the documentType option); pass
     * 'returnCursor' => true to receive the underlying cursor instead.
     *
     * @param array<string, mixed> $options Table query options
     * @param array<string, mixed> $filter
     *
     * @return ($options is array{returnCursor: true} ? array{head: array{count: int, offset: int|null, limit: int|null}, results: CursorInterface<int, array<string, mixed>|\ArrayObject<string, mixed>>} : array{head: array{count: int, offset: int|null, limit: int|null}, results: list<array<string, mixed>|\ArrayObject<string, mixed>>})
     */
    public function getTableRows(
        string $tableSpecId,
        array $filter = [],
        ?array $sortBy = [],
        ?int $offset = 0,
        ?int $limit = 10,
        array $options = []
    ): array {
        $t = new Timer();
        $t->start();

        $options = array_merge(
            [
                'returnCursor' => false,
                'includeCount' => true,
                'documentType' => \Tripod\Mongo\Documents\Tables::class,
            ],
            $options
        );

        $filter[_ID_KEY . '.' . _ID_TYPE] = $tableSpecId;

        $collection = $this->getConfigInstance()->getCollectionForTable(
            $this->storeName,
            $tableSpecId,
            $this->readPreference
        );

        $findOptions = [];
        if (!empty($limit)) {
            $findOptions['skip'] = $offset ?? 0;
            $findOptions['limit'] = $limit;
        }

        if (isset($sortBy)) {
            $findOptions['sort'] = $sortBy;
        }

        $results = $collection->find($filter, $findOptions);

        $count = $options['includeCount'] ? $collection->count($filter) : -1;

        $results->setTypeMap(['root' => $options['documentType'], 'document' => 'array', 'array' => 'array']);

        $t->stop();
        $this->timingLog(
            MONGO_TABLE_ROWS,
            ['duration' => $t->result(), 'query' => $filter, 'collection' => TABLE_ROWS_COLLECTION]
        );
        $this->getStat()->timer(MONGO_TABLE_ROWS . ('.' . $tableSpecId), $t->result());

        return [
            'head' => [
                'count' => $count,
                'offset' => $offset,
                'limit' => $limit,
            ],
            'results' => $options['returnCursor'] ? $results : array_values($results->toArray()),
        ];
    }

    /**
     * Returns the distinct values for a table column, optionally filtered by query.
     *
     * @param array<string, mixed> $filter
     *
     * @return array<string, mixed[]>
     */
    public function distinct(string $tableSpecId, string $fieldName, array $filter = []): array
    {
        $t = new Timer();
        $t->start();

        $filter[_ID_KEY . '.' . _ID_TYPE] = $tableSpecId;

        $collection = $this->config->getCollectionForTable($this->storeName, $tableSpecId, $this->readPreference);
        $results = $collection->distinct($fieldName, $filter);

        $t->stop();
        $query = ['distinct' => $fieldName, 'filter' => $filter];
        $this->timingLog(MONGO_TABLE_ROWS_DISTINCT, ['duration' => $t->result(), 'query' => $query, 'collection' => TABLE_ROWS_COLLECTION]);
        $this->getStat()->timer(MONGO_TABLE_ROWS_DISTINCT . ('.' . $tableSpecId), $t->result());

        return [
            'head' => [
                'count' => count($results),
            ],
            'results' => $results,
        ];
    }

    /**
     * This method will delete all table rows where the _id.type matches the specified $tableId.
     *
     * @param string               $tableId   Table spec ID
     * @param int|UTCDateTime|null $timestamp Optional timestamp to delete all table rows that are older than
     *
     * @return int The number of table rows deleted
     */
    public function deleteTableRowsByTableId(string $tableId, $timestamp = null): int
    {
        $t = new Timer();
        $t->start();

        $tableSpec = $this->getConfigInstance()->getTableSpecification($this->storeName, $tableId);
        if ($tableSpec == null) {
            $this->debugLog('Could not find a table specification for ' . $tableId);

            return 0;
        }

        $query = ['_id.type' => $tableId];
        if ($timestamp) {
            if (!$timestamp instanceof UTCDateTime) {
                $timestamp = DateUtil::getMongoDate($timestamp);
            }

            $query['$or'] = [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ];
        }

        $deleteResult = $this->getCollectionForTableSpec($tableId)
            ->deleteMany($query);

        $t->stop();
        $this->timingLog(MONGO_DELETE_TABLE_ROWS, ['duration' => $t->result(), 'query' => $query]);

        return $deleteResult->getDeletedCount();
    }

    /**
     * This method finds all the table specs for the given $rdfType and generates the table rows for the $subject one by one.
     */
    public function generateTableRowsForType(string $rdfType, string $subject, string $context, array $specTypes = []): void
    {
        $rdfType = $this->labeller->qname_to_alias($rdfType);
        $rdfTypeAlias = $this->labeller->uri_to_alias($rdfType);
        $foundSpec = false;

        if (empty($specTypes)) {
            $tableSpecs = $this->getConfigInstance()->getTableSpecifications($this->storeName);
        } else {
            $tableSpecs = [];
            foreach ($specTypes as $specType) {
                $spec = $this->getConfigInstance()->getTableSpecification($this->storeName, $specType);
                if ($spec) {
                    $tableSpecs[$specType] = $spec;
                }
            }
        }

        foreach ($tableSpecs as $key => $tableSpec) {
            if (isset($tableSpec[_ID_TYPE])) {
                $types = $tableSpec[_ID_TYPE];
                if (!is_array($types)) {
                    $types = [$types];
                }

                if (in_array($rdfType, $types) || in_array($rdfTypeAlias, $types)) {
                    $foundSpec = true;
                    $this->debugLog('Processing ' . $tableSpec[_ID_KEY]);
                    $this->generateTableRows($key, $subject, $context);
                }
            }
        }

        if (!$foundSpec) {
            $this->debugLog(sprintf("Could not find any table specifications for %s with resource type '%s'", $subject, $rdfType));

            return;
        }
    }

    /**
     * @param string|null $queueName Queue for background bulk generation
     */
    public function generateTableRows(string $tableType, ?string $resource = null, ?string $context = null, ?string $queueName = null): ?array
    {
        $t = new Timer();
        $t->start();

        $this->temporaryFields = [];
        $tableSpec = $this->getConfigInstance()->getTableSpecification($this->storeName, $tableType);
        $collection = $this->getConfigInstance()->getCollectionForTable($this->storeName, $tableType);

        if (empty($tableSpec)) {
            $this->debugLog('Could not find a table specification for ' . $tableType);

            return null;
        }

        // default the context
        $contextAlias = $this->getContextAlias($context);

        // default collection
        $from = $tableSpec['from'] ?? $this->podName;

        $types = [];
        if (is_array($tableSpec[_ID_TYPE])) {
            foreach ($tableSpec[_ID_TYPE] as $type) {
                $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($type)];
                $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($type)];
            }
        } else {
            $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($tableSpec[_ID_TYPE])];
            $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($tableSpec[_ID_TYPE])];
        }

        $filter = ['$or' => $types];
        if (isset($resource)) {
            $filter[_ID_KEY] = [
                _ID_RESOURCE => $this->labeller->uri_to_alias($resource),
                _ID_CONTEXT => $contextAlias,
            ];
        }

        // @todo Change this to a command when we upgrade MongoDB to 1.1+
        $count = $this->getConfigInstance()->getCollectionForCBD($this->storeName, $from)->count($filter);
        $docs = $this->getConfigInstance()
            ->getCollectionForCBD($this->storeName, $from)
            ->find($filter, ['maxTimeMS' => 1000000]);

        $jobOptions = [];
        $subjects = [];
        if ($queueName && !$resource && ($this->stat || $this->statsConfig !== [])) {
            $jobOptions['statsConfig'] = $this->getStatsConfig();
            $jobGroup = $this->getJobGroup($this->storeName);
            $jobOptions[ApplyOperation::TRACKING_KEY] = $jobGroup->getId()->__toString();
            $jobGroup->setJobCount($count);
        }

        foreach ($docs as $doc) {
            if ($queueName && !$resource) {
                $subject = new ImpactedSubject(
                    $doc[_ID_KEY],
                    OP_TABLES,
                    $this->storeName,
                    $from,
                    [$tableType]
                );
                $subjects[] = $subject;
                if (count($subjects) >= $this->getConfigInstance()->getBatchSize(OP_TABLES)) {
                    $this->queueApplyJob($subjects, $queueName, $jobOptions);
                    $subjects = [];
                }
            } else {
                // set up ID
                $generatedRow = [
                    _ID_KEY => [
                        _ID_RESOURCE => $doc[_ID_KEY][_ID_RESOURCE],
                        _ID_CONTEXT => $doc[_ID_KEY][_ID_CONTEXT],
                        _ID_TYPE => $tableSpec[_ID_KEY],
                    ],
                    \_CREATED_TS => DateUtil::getMongoDate(),
                ];
                // everything must go in the value object todo: this is a hang over from map reduce days, engineer out once we have stability on new PHP method for M/R
                $value = [_ID_KEY => $doc[_ID_KEY]];
                $this->addIdToImpactIndex($doc[_ID_KEY], $value); // need to add the doc to the impact index to be consistent with views/search etc. this is needed for discovering impacted operations
                $this->addFields($doc, $tableSpec, $value);
                if (isset($tableSpec['joins'])) {
                    $this->doJoins($doc, $tableSpec['joins'], $value, $from, $contextAlias);
                }

                if (isset($tableSpec['counts'])) {
                    $this->doCounts($doc, $tableSpec['counts'], $value);
                }

                if (isset($tableSpec['computed_fields'])) {
                    $this->doComputedFields($tableSpec, $value);
                }

                // Remove temp fields from document

                $generatedRow['value'] = array_diff_key($value, array_flip($this->temporaryFields));
                $this->truncatingSave($collection, $generatedRow);
            }
        }

        if ($subjects !== []) {
            $this->queueApplyJob($subjects, $queueName, $jobOptions);
        }

        $t->stop();
        $this->timingLog(MONGO_CREATE_TABLE, [
            'type' => $tableSpec[_ID_TYPE],
            'duration' => $t->result(),
            'filter' => $filter,
            'from' => $from,
        ]);
        $this->getStat()->timer(MONGO_CREATE_TABLE . ('.' . $tableType), $t->result());

        $stat = ['count' => $count];
        if (isset($jobOptions[ApplyOperation::TRACKING_KEY])) {
            $stat[ApplyOperation::TRACKING_KEY] = $jobOptions[ApplyOperation::TRACKING_KEY];
        }

        return $stat;
    }

    /**
     * Count the number of documents in the spec that match $filters.
     *
     * @param string               $tableSpec Table spec ID
     * @param array<string, mixed> $filters   Query filters to get count on
     *
     * @return int
     */
    public function count(string $tableSpec, array $filters = [])
    {
        $filters['_id.type'] = $tableSpec;

        return $this->getCollectionForTableSpec($tableSpec)->count($filters);
    }

    /**
     * @param string            $resource The URI or alias of the resource to delete from tables
     * @param string|null       $context  Optional context
     * @param array|string|null $specType Optional table type or array of table types to delete from
     */
    protected function deleteTableRowsForResource(string $resource, ?string $context = null, $specType = null): void
    {
        $t = new Timer();
        $t->start();

        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);
        $query = [_ID_KEY . '.' . _ID_RESOURCE => $resourceAlias, _ID_KEY . '.' . _ID_CONTEXT => $contextAlias];
        $specNames = [];
        $specTypes = $this->config->getTableSpecifications($this->storeName);
        if (empty($specType)) {
            $specNames = array_keys($specTypes);
        } elseif (is_string($specType)) {
            $query[_ID_KEY][_ID_TYPE] = $specType;
            $specNames = [$specType];
        } elseif (is_array($specType)) {
            $query[_ID_KEY . '.' . _ID_TYPE] = ['$in' => $specType];
            $specNames = $specType;
        }

        foreach ($specNames as $specName) {
            // Ignore any other types of specs that might have been passed in here
            if (isset($specTypes[$specName])) {
                $this->config->getCollectionForTable($this->storeName, $specName)->deleteMany($query);
            }
        }

        $t->stop();
        $this->timingLog(MONGO_DELETE_TABLE_ROWS, ['duration' => $t->result(), 'query' => $query, 'collection' => TABLE_ROWS_COLLECTION]);
    }

    /**
     * This method handles invalidation and regeneration of table rows based on impact index, before delegating to
     * generateTableRowsForType() for re-generation of any table rows for the $resource.
     */
    protected function generateTableRowsForResource(string $resource, ?string $context = null, array $specTypes = []): void
    {
        $resourceAlias = $this->labeller->uri_to_alias($resource);
        $contextAlias = $this->getContextAlias($context);

        $this->deleteTableRowsForResource($resource, $context, $specTypes);

        $filter = [];
        $filter[] = [_ID_RESOURCE => $resourceAlias, _ID_CONTEXT => $contextAlias];

        // now go through the types
        $query = [_ID_KEY => ['$in' => $filter]];
        $resourceAndType = $this->config->getCollectionForCBD($this->storeName, $this->podName)
            ->find($query, ['projection' => [_ID_KEY => 1, 'rdf:type' => 1]]);

        foreach ($resourceAndType as $rt) {
            $id = $rt[_ID_KEY];
            if (isset($rt['rdf:type'])) {
                if (isset($rt['rdf:type'][VALUE_URI])) {
                    // single type, not an array of values
                    $this->generateTableRowsForType($rt['rdf:type'][VALUE_URI], $id[_ID_RESOURCE], $id[_ID_CONTEXT], $specTypes);
                } else {
                    // an array of types
                    foreach ($rt['rdf:type'] as $type) {
                        // Defensive check in case there is bad data for rdf:type
                        if (isset($type[VALUE_URI])) {
                            $this->generateTableRowsForType($type[VALUE_URI], $id[_ID_RESOURCE], $id[_ID_CONTEXT], $specTypes);
                        }
                    }
                }
            }
        }
    }

    /**
     * Save the generated rows to the given collection.
     *
     * If an exception in thrown because a field is too large to index, the field is
     * truncated and the save is retried.
     *
     * @param array<string, mixed> $generatedRow the rows to save
     *
     * @throws \Exception
     */
    protected function truncatingSave(Collection $collection, array $generatedRow): void
    {
        try {
            $this->upsertGeneratedRow($collection, $generatedRow);
        } catch (\Exception $e) {
            // We only truncate and retry the save if the \Exception contains this text.
            if (strpos($e->getMessage(), '::insert: key too large to index') !== false) {
                $this->truncateFields($collection, $generatedRow);
                $this->upsertGeneratedRow($collection, $generatedRow);
            } else {
                throw $e;
            }
        }
    }

    /**
     * Truncate any indexed fields in the generated rows which are too large to index.
     *
     * [NOTE]:  Starting in version 4.2, MongoDB removes the Index Key Limit for
     *          featureCompatibilityVersion (fCV) set to "4.2" or greater.
     *
     * For MongoDB 2.6 through MongoDB versions with fCV set to "4.0" or earlier,
     * the total size of an index entry, which can include structural overhead
     * depending on the BSON type, must be less than 1024 bytes.
     *
     * @param array<string, mixed> $generatedRow - Pass by reference so that the contents is truncated
     */
    protected function truncateFields(Collection $collection, array &$generatedRow): void
    {
        // Find the name of any indexed fields
        $indexedFields = [];
        $indexesGroupedByCollection = $this->config->getIndexesGroupedByCollection($this->storeName);
        if (isset($indexesGroupedByCollection[$collection->getCollectionName()])) {
            foreach ($indexesGroupedByCollection[$collection->getCollectionName()] as $repset) {
                foreach ($repset as $index) {
                    foreach ($index as $indexedFieldname => $v) {
                        if (strpos($indexedFieldname, 'value.') === 0) {
                            $indexedFields[] = substr($indexedFieldname, strlen('value.'));
                        }
                    }
                }
            }
        }

        if ($indexedFields !== [] && isset($generatedRow['value']) && is_array($generatedRow['value'])) {
            $value = &$generatedRow['value'];
            foreach ($indexedFields as $indexedFieldname) {
                // The key will have the index name in the following format added to it.
                // Adjust the max key size allowed to take it into account.
                $maxKeySize = 1020 - strlen('value_' . $indexedFieldname . '_1');

                // It's important that we count the number of bytes
                // in the field - not just the number of characters.
                // UTF-8 characters can be between 1 and 4 bytes.
                //
                // From the strlen documentation:
                //     Attention with utf8:
                //     $foo = "bär";
                //     strlen($foo) will return 4 and not 3 as expected..
                //
                // So strlen does count the bytes - not the characters.
                if (array_key_exists($indexedFieldname, $value) && (is_string($value[$indexedFieldname]) && strlen($value[$indexedFieldname]) > $maxKeySize)) {
                    $value[$indexedFieldname] = substr($value[$indexedFieldname], 0, $maxKeySize);
                }
            }
        }
    }

    /**
     * @param array<string, mixed> $spec The table spec
     * @param mixed[]              $dest The table row document to save
     */
    protected function doComputedFields(array $spec, array &$dest): void
    {
        if (isset($spec['computed_fields'])) {
            foreach ($spec['computed_fields'] as $f) {
                if (isset($f['fieldName'], $f['value']) && is_array($f['value'])) {
                    if (isset($f['temporary']) && $f['temporary'] === true && !in_array($f['fieldName'], $this->temporaryFields)) {
                        $this->temporaryFields[] = $f['fieldName'];
                    }

                    $computedFunctions = array_values(array_intersect(self::$computedFieldFunctions, array_keys($f['value'])));
                    $dest[$f['fieldName']] = $this->getComputedValue($computedFunctions[0], $f['value'], $dest);
                }
            }
        }
    }

    /**
     * @param string               $function A defined computed value function
     * @param array<string, mixed> $spec     The computed field spec
     * @param array<string, mixed> $dest     The table row document to save
     *
     * @return mixed The computed value
     */
    protected function getComputedValue(string $function, array $spec, array &$dest)
    {
        $value = null;

        switch ($function) {
            case 'conditional':
                $value = $this->generateConditionalValue($spec[$function], $dest);

                break;

            case 'replace':
                $value = $this->generateReplaceValue($spec[$function], $dest);

                break;

            case 'arithmetic':
                $value = $this->computeArithmeticValue($spec[$function], $dest);

                break;
        }

        return $value;
    }

    /**
     * @param array<int, mixed>    $equation
     * @param array<string, mixed> $dest
     *
     * @return float|int|null
     *
     * @throws \InvalidArgumentException
     */
    protected function computeArithmeticValue(array $equation, array &$dest)
    {
        if (count($equation) < 3) {
            throw new \InvalidArgumentException('Equations must consist of an array with 3 values');
        }

        if (!in_array($equation[1], self::$arithmeticOperators)) {
            throw new \InvalidArgumentException('Invalid arithmetic operator');
        }

        $left = $this->rewriteVariableValue($equation[0], $dest, 'numeric');
        $right = $this->rewriteVariableValue($equation[2], $dest, 'numeric');
        if (is_array($left)) {
            $left = $this->computeArithmeticValue($left, $dest);
        }

        if (is_array($right)) {
            $right = $this->computeArithmeticValue($right, $dest);
        }

        switch ($equation[1]) {
            case '+':
                $value = $left + $right;

                break;

            case '-':
                $value = $left - $right;

                break;

            case '*':
                $value = $left * $right;

                break;

            case '/':
                $value = $left / $right;

                break;

            case '%':
                $value = $left % $right;

                break;

            default:
                $value = null;
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $replaceSpec The replace value spec
     * @param array<string, mixed> $dest        The table row document to save
     *
     * @return mixed
     */
    protected function generateReplaceValue(array $replaceSpec, array &$dest)
    {
        $search = null;
        $replace = null;
        $subject = null;
        if (isset($replaceSpec['search'])) {
            $search = $this->rewriteVariableValue($replaceSpec['search'], $dest);
        }

        if (isset($replaceSpec['replace'])) {
            $replace = $this->rewriteVariableValue($replaceSpec['replace'], $dest);
        }

        if (isset($replaceSpec['subject'])) {
            $subject = $this->rewriteVariableValue($replaceSpec['subject'], $dest);
        }

        return str_replace($search, $replace, $subject);
    }

    /**
     * @param array<string, mixed> $conditionalSpec The conditional spec
     * @param array<string, mixed> $dest            The table row document to save
     *
     * @return mixed The computed value
     */
    protected function generateConditionalValue(array $conditionalSpec, array &$dest)
    {
        $value = null;
        if (isset($conditionalSpec['if']) && is_array($conditionalSpec['if'])) {
            $left = null;
            $operator = null;
            $right = null;
            if (isset($conditionalSpec['if'][0])) {
                $left = $this->rewriteVariableValue($conditionalSpec['if'][0], $dest);
            }

            if (isset($conditionalSpec['if'][1])) {
                $operator = $conditionalSpec['if'][1];
            }

            if (isset($conditionalSpec['if'][2])) {
                $right = $this->rewriteVariableValue($conditionalSpec['if'][2], $dest);
            }

            $bool = $this->doConditional($left, $operator, $right);

            $path = ($bool ? 'then' : 'else');

            if (isset($conditionalSpec[$path])) {
                if (is_array($conditionalSpec[$path])) {
                    $nestedComputedFunctions = array_intersect(self::$computedFieldFunctions, array_keys($conditionalSpec[$path]));
                    // This is 'just a regular old array'
                    if ($nestedComputedFunctions === []) {
                        return $this->rewriteVariableValue($conditionalSpec[$path], $dest);
                    }

                    return $this->getComputedValue($nestedComputedFunctions[0], $conditionalSpec[$path], $dest);
                }

                return $this->rewriteVariableValue($conditionalSpec[$path], $dest);
            }
        }

        return $value;
    }

    /**
     * @param mixed                $value   The value to replace, if it contains a variable
     * @param array<string, mixed> $dest    The table row document to save
     * @param string|null          $setType Force the return to be set to specified type
     *
     * @return mixed
     */
    protected function rewriteVariableValue($value, array &$dest, ?string $setType = null)
    {
        if (is_string($value)) {
            if (strpos($value, '$') === 0) {
                $key = str_replace('$', '', $value);
                if (isset($dest[$key])) {
                    return $this->castValueType($dest[$key], $setType);
                }

                return null;
            }

            return $this->castValueType($value, $setType);
        }

        if (is_array($value)) {
            if ($this->isFunction($value)) {
                $function = array_keys($value);

                return $this->getComputedValue($function[0], $value, $dest);
            }

            $aryValue = [];
            foreach ($value as $v) {
                $aryValue[] = $this->rewriteVariableValue($v, $dest);
            }

            return $aryValue;
        }

        return $this->castValueType($value, $setType);
    }

    /**
     * @param mixed $value
     */
    protected function isFunction($value): bool
    {
        return is_array($value) && count(array_keys($value)) === 1 && count(array_intersect(array_keys($value), self::$computedFieldFunctions)) === 1;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    protected function castValueType($value, ?string $type = null)
    {
        // If value is a UTCDateTime, turn into a DateTime object in order to perform comparison
        if ($value instanceof UTCDateTime) {
            $value = $value->toDateTime();
        }

        switch ($type) {
            case 'string':
                $value = (string) $value;

                break;

            case 'bool':
            case 'boolean':
                $value = (bool) $value;

                break;

            case 'numeric':
                if ((!is_int($value)) && !is_float($value)) {
                    $value = $value == (string) (int) $value ? (int) $value : (float) $value;
                }

                break;
        }

        return $value;
    }

    /**
     * @param mixed       $left     The left value of the condition
     * @param string|null $operator The comparison operator
     * @param mixed|null  $right    The right value of the condition
     *
     * @throws \InvalidArgumentException
     */
    protected function doConditional($left, ?string $operator, $right): bool
    {
        if (!$operator) {
            return (bool) $left;
        }

        if (!in_array($operator, self::$conditionalOperators)) {
            throw new \InvalidArgumentException('Invalid conditional operator');
        }

        $result = false;

        switch ($operator) {
            case '>':
                $result = $left > $right;

                break;

            case '>=':
                $result = $left >= $right;

                break;

            case '<':
                $result = $left < $right;

                break;

            case '<=':
                $result = $left <= $right;

                break;

            case '==':
                $result = $left == $right;

                break;

            case '!=':
                $result = $left != $right;

                break;

            case 'contains':
            case 'not contains':
                $bool = is_array($left) ? in_array($right, $left) : strpos((string) $left, (string) $right) !== false;

                $result = ($bool && $operator !== 'not contains');

                break;

            case '~=':
            case '!~':
                $match = preg_match($right, $left);
                $result = ($match > 0 && $operator !== '!~');

                break;
        }

        return $result;
    }

    /**
     * Add fields to a table row.
     *
     * @param array<string, mixed> $source
     * @param array<string, mixed> $spec
     * @param array<string, mixed> $dest
     */
    protected function addFields(array $source, array $spec, array &$dest): void
    {
        if (isset($spec['fields'])) {
            foreach ($spec['fields'] as $f) {
                /** @var string */
                $fieldName = $f['fieldName'];

                if (isset($f['temporary']) && $f['temporary'] === true && !in_array($fieldName, $this->temporaryFields)) {
                    $this->temporaryFields[] = $fieldName;
                }

                if (isset($f['predicates'])) {
                    foreach ($f['predicates'] as $p) {
                        if (is_string($p) && isset($source[$p])) {
                            // Predicate is referenced directly
                            $this->generateValues($source, $f, $p, $dest);
                        } else {
                            // Get a list of functions to run over a predicate - reverse it
                            $predicateFunctions = $this->getPredicateFunctions($p);
                            $predicateFunctions = array_reverse($predicateFunctions);

                            foreach ($predicateFunctions as $function => $functionOptions) {
                                // If we've got values then we're the innermost function, so we need to get the values
                                if ($function == 'predicates') {
                                    foreach ($functionOptions as $v) {
                                        $v = trim($v);
                                        if (isset($source[$v])) {
                                            $this->generateValues($source, $f, $v, $dest);
                                        }
                                    }

                                // Otherwise apply a modifier
                                } elseif (isset($dest[$fieldName])) {
                                    $dest[$fieldName] = $this->applyModifier($function, $dest[$fieldName], $functionOptions);
                                }
                            }
                        }
                    }
                }

                // Allow URI linking to the ID
                if (isset($f['value']) && ($f['value'] == '_link_' || $f['value'] == 'link')) {
                    if ($f['value'] == '_link_') {
                        $this->warningLog("Table spec value '_link_' is deprecated", $f);
                    }

                    // If value exists, set as array
                    if (isset($dest[$fieldName])) {
                        if (!is_array($dest[$fieldName])) {
                            $dest[$fieldName] = [$dest[$fieldName]];
                        }

                        $dest[$fieldName][] = $this->labeller->qname_to_alias($source[_ID_KEY][_ID_RESOURCE]);
                    } else {
                        $dest[$fieldName] = $this->labeller->qname_to_alias($source[_ID_KEY][_ID_RESOURCE]);
                    }
                }
            }
        }
    }

    /**
     * Generate values for a given predicate.
     *
     * @param array<string, mixed> $source
     * @param array<string, mixed> $f
     * @param array<string, mixed> $dest
     */
    protected function generateValues(array $source, array $f, string $predicate, array &$dest): void
    {
        $values = [];
        if (isset($source[$predicate][VALUE_URI]) && !empty($source[$predicate][VALUE_URI])) {
            $values[] = $source[$predicate][VALUE_URI];
        } elseif (isset($source[$predicate][VALUE_LITERAL]) && !empty($source[$predicate][VALUE_LITERAL])) {
            $values[] = $source[$predicate][VALUE_LITERAL];
        } elseif (isset($source[$predicate][_ID_RESOURCE])) { // field being joined is the _id, will have _id{r:'',c:''}
            $values[] = $source[$predicate][_ID_RESOURCE];
        } else {
            foreach ($source[$predicate] as $v) {
                if (isset($v[VALUE_LITERAL]) && !empty($v[VALUE_LITERAL])) {
                    $values[] = $v[VALUE_LITERAL];
                } elseif (isset($v[VALUE_URI]) && !empty($v[VALUE_URI])) {
                    $values[] = $v[VALUE_URI];
                }

                // _id's shouldn't appear in value arrays, so no need for third condition here
            }
        }

        /** @var string */
        $fieldName = $f['fieldName'];

        // now add all the values
        foreach ($values as $v) {
            if (!isset($dest[$fieldName])) {
                // single value
                $dest[$fieldName] = $v;
            } elseif (is_array($dest[$fieldName])) {
                // add to existing array of values
                $dest[$fieldName][] = $v;
            } else {
                // convert from single value to array of values
                $existingVal = $dest[$fieldName];
                $dest[$fieldName] = [];
                $dest[$fieldName][] = $existingVal;
                $dest[$fieldName][] = $v;
            }
        }
    }

    /**
     * Recursively get functions that can modify a predicate.
     *
     * @param array|string $array
     */
    protected function getPredicateFunctions($array): array
    {
        $predicateFunctions = [];
        if (is_array($array)) {
            if (isset($array['predicates'])) {
                $predicateFunctions['predicates'] = $array['predicates'];
            } elseif ($array !== []) {
                $function = key($array);
                $predicateFunctions[$function] = $array[$function];
                if (is_array($array[$function])) {
                    $predicateFunctions = array_merge($predicateFunctions, $this->getPredicateFunctions($array[$function]));
                }
            }
        }

        return $predicateFunctions;
    }

    protected function doJoins(array $source, array $joins, array &$dest, string $from, string $contextAlias): void
    {
        $this->expandSequence($joins, $source);
        foreach ($joins as $predicate => $ruleset) {
            if (isset($source[$predicate])) {
                // todo: perhaps we can get better performance by detecting whether or not
                // the uri to join on is already in the impact index, and if so not attempting
                // to join on it. However, we need to think about different combinations of
                // nested joins in different points of the view spec and see if this would
                // complicate things. Needs a unit test or two.
                $joinUris = [];
                if (isset($source[$predicate][VALUE_URI])) {
                    // single value for join
                    $joinUris[] = [_ID_RESOURCE => $source[$predicate][VALUE_URI], _ID_CONTEXT => $contextAlias];
                } elseif ($predicate == _ID_KEY) {
                    // single value for join
                    $joinUris[] = [_ID_RESOURCE => $source[$predicate][_ID_RESOURCE], _ID_CONTEXT => $contextAlias];
                } else {
                    // multiple values for join
                    foreach ($source[$predicate] as $v) {
                        $joinUris[] = [_ID_RESOURCE => $v[VALUE_URI], _ID_CONTEXT => $contextAlias];
                    }
                }

                $recursiveJoins = [];
                $collection = (
                    isset($ruleset['from'])
                    ? $this->config->getCollectionForCBD($this->storeName, $ruleset['from'])
                    : $this->config->getCollectionForCBD($this->storeName, $from)
                );

                $cursor = $collection->find([_ID_KEY => ['$in' => $joinUris]], [
                    'maxTimeMS' => 1000000,
                ]);

                $this->addIdToImpactIndex($joinUris, $dest);
                foreach ($cursor as $linkMatch) {
                    $this->addFields($linkMatch, $ruleset, $dest);

                    if (isset($ruleset['counts'])) {
                        $this->doCounts($linkMatch, $ruleset['counts'], $dest);
                    }

                    if (isset($ruleset['joins'])) {
                        // recursive joins must be done after this cursor has completed, otherwise things get messy
                        $recursiveJoins[] = ['data' => $linkMatch, 'ruleset' => $ruleset['joins']];
                    }
                }

                foreach ($recursiveJoins as $r) {
                    $this->doJoins($r['data'], $r['ruleset'], $dest, $from, $contextAlias);
                }
            }
        }
    }

    /**
     * Add counts to $dest by counting what is in $source according to $countSpec.
     *
     * @param int[] $dest
     */
    protected function doCounts(array $source, array $countSpec, array &$dest): void
    {
        // process count aggregate function
        foreach ($countSpec as $c) {
            $fieldName = $c['fieldName'];
            if (isset($c['temporary']) && $c['temporary'] === true && !in_array($fieldName, $this->temporaryFields)) {
                $this->temporaryFields[] = $fieldName;
            }

            $applyRegex = isset($c['regex']) ?: null;
            $count = 0;
            // just count predicates at current location
            if (isset($source[$c['property']])) {
                if (isset($source[$c['property']][VALUE_URI]) || isset($source[$c['property']][VALUE_LITERAL])) {
                    $count = $applyRegex != null ? $this->applyRegexToValue($c['regex'], $source[$c['property']]) : 1;
                } elseif ($applyRegex != null) {
                    foreach ($source[$c['property']] as $value) {
                        if ($this->applyRegexToValue($c['regex'], $value)) {
                            $count++;
                        }
                    }
                } else {
                    $count = count($source[$c['property']]);
                }
            }

            if (!isset($dest[$fieldName])) {
                $dest[$fieldName] = 0;
            }

            $dest[$fieldName] += $count;
        }
    }

    /**
     * Test if the a particular type appears in the array of types associated with a particular spec and that the changeset
     * includes rdf:type (or is empty, meaning addition or deletion vs. update).
     */
    protected function checkIfTypeShouldTriggerOperation(string $rdfType, array $validTypes, array $subjectPredicates): bool
    {
        // We don't know if this is an alias or a fqURI, nor what is in the valid types, necessarily
        $types = [$rdfType];

        try {
            $types[] = $this->labeller->qname_to_uri($rdfType);
        } catch (LabellerException $e) {
        }

        try {
            $types[] = $this->labeller->uri_to_alias($rdfType);
        } catch (LabellerException $e) {
        }

        $intersectingTypes = array_unique(array_intersect($types, $validTypes));
        if ($intersectingTypes !== []) {
            // Table rows only need to be invalidated if their rdf:type property has changed
            // This means we're either adding or deleting a graph
            if ($subjectPredicates === []) {
                return true;
            }

            // Check for alias in changed predicates
            if (in_array('rdf:type', $subjectPredicates)) {
                return true;
            }

            // Check for fully qualified URI in changed predicates
            if (in_array(RDF_TYPE, $subjectPredicates)) {
                return true;
            }
        }

        return false;
    }

    /**
     * For mocking.
     *
     * @param string $tableSpecId Table spec ID
     */
    protected function getCollectionForTableSpec(string $tableSpecId): Collection
    {
        return $this->getConfigInstance()->getCollectionForTable($this->storeName, $tableSpecId);
    }

    /**
     * Apply a specific modifier
     * Options you can use are
     *      lowercase - no options
     *      join - pass in "glue":" " to specify what to glue multiple values together with
     *      date - no options.
     *
     * @param string|string[]|\Stringable|\Stringable[] $value
     * @param array<string, bool|int|string>            $options
     *
     * @return mixed
     *
     * @throws \Exception
     */
    private function applyModifier(string $modifier, $value, array $options = [])
    {
        switch ($modifier) {
            case 'predicates':
                // Used to generate a list of values - does nothing here
                break;

            case 'lowercase':
                $value = is_array($value) ? array_map([$this, 'strtolower'], $value) : $this->strtolower($value);

                break;

            case 'join':
                if (is_array($value)) {
                    $value = implode((string) $options['glue'], $value);
                }

                break;

            case 'date':
                if (is_string($value)) {
                    $value = DateUtil::getMongoDate(strtotime($value) * 1000);
                }

                break;

            default:
                throw new \Exception('Could not apply modifier:' . $modifier);
        }

        return $value;
    }

    /**
     * Lowercase a value, casting to string first.
     *
     * @param string|\Stringable $value
     */
    private function strtolower($value): string
    {
        return strtolower((string) $value);
    }

    /**
     * Apply a regex to the RDF property value defined in $value.
     *
     * @param array<string, mixed> $value
     *
     * @return false|int
     *
     * @throws \Tripod\Exceptions\Exception
     */
    private function applyRegexToValue(string $regex, array $value)
    {
        if (isset($value[VALUE_URI]) || isset($value[VALUE_LITERAL])) {
            $v = $value[VALUE_URI] ?: $value[VALUE_LITERAL];

            return preg_match($regex, $v);
        }

        throw new \Tripod\Exceptions\Exception('Was expecting either VALUE_URI or VALUE_LITERAL when applying regex to value - possible data corruption with: ' . var_export($value, true));
    }

    private function upsertGeneratedRow(Collection $collection, array $generatedRow): void
    {
        try {
            $collection->updateOne([_ID_KEY => $generatedRow[_ID_KEY]], ['$set' => $generatedRow], ['upsert' => true]);
        } catch (BulkWriteException $e) {
            if ($this->isDuplicateKeyError($e)) {
                $existingRow = $collection->findOne([_ID_KEY => $generatedRow[_ID_KEY]]);
                $this->getLogger()->warning('Duplicate key error when upserting generated table row, retrying.', [
                    'error' => $e,
                    'generatedRow' => $generatedRow,
                    'existingRow' => $existingRow,
                ]);
                $collection->updateOne([_ID_KEY => $generatedRow[_ID_KEY]], ['$set' => $generatedRow], ['upsert' => false]);
            } else {
                throw $e;
            }
        }
    }

    private function isDuplicateKeyError(BulkWriteException $error): bool
    {
        return $error->getCode() === 11000 || strpos($error->getMessage(), 'E11000') !== false;
    }
}
