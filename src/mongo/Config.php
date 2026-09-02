<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\ConnectionTimeoutException;
use MongoDB\Driver\ReadPreference;
use Psr\Log\LoggerInterface;
use Tripod\Exceptions\ConfigException;
use Tripod\ISearchProvider;
use Tripod\Mongo\Composites\Tables;

/**
 * Holds the global configuration for Tripod.
 */
class Config implements IConfigInstance
{
    public const VALIDATE_MIN = 'MIN';

    public const VALIDATE_MAX = 'MAX';

    public const CONNECTION_RETRIES = 30;

    /**
     * All of the defined searchDocSpecs.
     */
    protected array $searchDocSpecs = [];

    /**
     * All defined namespaces.
     *
     * @var array<string, string> An associative array of namespaces, keyed by prefix
     */
    protected array $ns = [];

    /**
     * The transaction log db config.
     */
    protected array $tConfig = [];

    /**
     * The value should be the name of a class that implement ISearchProvider keyed by storename.
     * The class name comes straight from config, so it is not guaranteed to reference a real class;
     * consumers validate with class_exists() before instantiating.
     *
     * @var array<string, string>
     */
    protected array $searchProviderClassName = [];

    /**
     * All of the predicates associated with a particular spec document.
     */
    protected array $specPredicates;

    /**
     * A simple map between collection names and the database name they belong to.
     */
    protected array $collectionDatabases = [];

    protected array $activeMongoConnections = [];

    protected array $dataSources = [];

    protected array $podConnections = [];

    protected static string $validationLevel = self::VALIDATE_MIN;

    /**
     * Database connections, keyed by datasource, so we're not inadvertently opening many db connections through getDatabase().
     */
    protected array $connections = [];

    protected int $mongoCursorTimeout = 30000;

    /**
     * @var array<string, int>
     */
    protected array $batchSize = [
        OP_TABLES => 100,
        OP_SEARCH => 100,
        OP_VIEWS => 25,
    ];

    private ?array $config = null;

    private ?Labeller $labeller = null;

    private string $defaultContext;

    /**
     * The defined database indexes, keyed by database name.
     */
    private array $indexes = [];

    private array $cardinality = [];

    /**
     * The connection strings for each defined database.
     */
    private array $dbConfig = [];

    /**
     * All of the defined viewSpecs.
     */
    private array $viewSpecs = [];

    /**
     * All of the defined tableSpecs.
     */
    private array $tableSpecs = [];

    /**
     * Defined database configuration: dbname, collections, etc.
     */
    private array $databases = [];

    /**
     * Config should not be instantiated directly: use Config::getInstance().
     */
    private function __construct() {}

    public function getMongoCursorTimeout(): int
    {
        return $this->mongoCursorTimeout;
    }

    public function setMongoCursorTimeout(int $mongoCursorTimeout): void
    {
        $this->mongoCursorTimeout = $mongoCursorTimeout;
    }

    /**
     * @param array<string, mixed> $spec
     *
     * @throws ConfigException
     */
    public function validateTableSpec(array $spec): void
    {
        if (!isset($spec[_ID_KEY])) {
            throw new ConfigException('Table spec does not contain ' . _ID_KEY);
        }

        if (!isset($spec['from'])) {
            throw new ConfigException('Table spec does not contain from');
        }

        $this->validateTableSpecPart($spec, 0);
    }

    public function getValidationLevel(): string
    {
        return self::$validationLevel;
    }

    public static function setValidationLevel(string $validationLevel): void
    {
        self::$validationLevel = $validationLevel;
    }

    /**
     * Returns an array of associated predicates in a table or search document specification
     * Note: will not return viewSpec predicates.
     */
    public function getDefinedPredicatesInSpec(string $storename, string $specId): array
    {
        if (!isset($this->specPredicates[$storename])) {
            $this->specPredicates[$storename] = $this->getDefinedPredicatesInSpecs($storename);
        }

        return $this->specPredicates[$storename][$specId] ?? [];
    }

    /**
     * Check modifier functions against fields.
     *
     * @param array|bool $parent
     *
     * @throws ConfigException
     */
    public function checkModifierFunctions(array $array, $parent, ?string $parentKey = null): void
    {
        foreach ($array as $k => $v) {
            // You can have recursive modifiers so we check if the value is an array.
            if (is_array($v)) {
                // Check config
                // Valid configs can be top level modifiers and their attributes inside - you can have a top level modifier
                // inside a top level modifier - that's why we also check \Tripod\Mongo\Composites\Tables::$predicatesModifiers direct
                // If this config value is a top level modifier, use that as the parent so that we can check the attributes
                if (isset(Tables::$predicateModifiers[$k])) {
                    $this->checkModifierFunctions($v, Tables::$predicateModifiers[$k], $k);
                } elseif (is_array($parent) && isset($parent[$k])) {
                    $this->checkModifierFunctions($v, $parent[$k], $k);
                } else {
                    throw new ConfigException("Invalid modifier: '" . $k . "' in key '" . $parentKey . "'");
                }
            } elseif (is_string($k)) {
                // Check key
                if (!isset($parent[$k])) {
                    throw new ConfigException("Invalid modifier: '" . $k . "' in key '" . $parentKey . "'");
                }
            }
        }
    }

    /**
     * Returns an alias curie of the default context (i.e. graph name).
     */
    public function getDefaultContextAlias(): string
    {
        return $this->getLabeller()->uri_to_alias($this->defaultContext);
    }

    /**
     * Since this is a singleton class, use this method to create a new config instance.
     *
     * @codeCoverageIgnore
     *
     * @deprecated
     *
     * @throws ConfigException
     */
    public static function getInstance(): IConfigInstance
    {
        self::getLogger()->warning(
            Config::class . '::getInstance deprecated, use ' . \Tripod\Config::class . '::getInstance instead'
        );

        return \Tripod\Config::getInstance();
    }

    /**
     * Sets the tripod configuration.
     *
     * @deprecated
     */
    public static function setConfig(array $config): void
    {
        self::getLogger()->warning(
            Config::class . '::setConfig deprecated, use ' . \Tripod\Config::class . '::setConfig instead'
        );
        \Tripod\Config::setConfig($config);
    }

    /**
     * Returns configuration array.
     *
     * @deprecated
     */
    public static function getConfig(): array
    {
        self::getLogger()->warning(
            Config::class . '::getConfig deprecated, use ' . \Tripod\Config::class . '::getConfig instead'
        );

        return \Tripod\Config::getConfig();
    }

    /**
     * Returns a list of the configured indexes grouped by collection.
     */
    public function getIndexesGroupedByCollection(string $storeName): array
    {
        $indexes = $this->indexes[$storeName];
        // TODO: if we have much more default indexes we should find a better way of doing this
        foreach ($indexes as $collection => $indices) {
            $indexes[$collection][_LOCKED_FOR_TRANS_INDEX] = [_ID_KEY => 1, _LOCKED_FOR_TRANS => 1];
            $indexes[$collection][_UPDATED_TS_INDEX] = [_ID_KEY => 1, _UPDATED_TS => 1];
            $indexes[$collection][_CREATED_TS_INDEX] = [_ID_KEY => 1, _CREATED_TS => 1];
        }

        // also add the indexes for any views/tables
        $tableIndexes = [];
        foreach ($this->getTableSpecifications($storeName) as $tspec) {
            if (isset($tspec['ensureIndexes'])) {
                // Indexes should be keyed by data_source
                if (!isset($tableIndexes[$tspec['to_data_source']])) {
                    $tableIndexes[$tspec['to_data_source']] = [];
                }

                foreach ($tspec['ensureIndexes'] as $index) {
                    $tableIndexes[$tspec['to_data_source']][] = $index;
                }
            }
        }

        $indexes[TABLE_ROWS_COLLECTION] = $tableIndexes;

        $viewIndexes = [];
        foreach ($this->getViewSpecifications($storeName) as $vspec) {
            if (isset($vspec['ensureIndexes'])) {
                // Indexes should be keyed by data_source
                if (!isset($viewIndexes[$vspec['to_data_source']])) {
                    $viewIndexes[$vspec['to_data_source']] = [];
                }

                foreach ($vspec['ensureIndexes'] as $index) {
                    $viewIndexes[$vspec['to_data_source']][] = $index;
                }
            }
        }

        $indexes[VIEWS_COLLECTION] = $viewIndexes;

        return $indexes;
    }

    /**
     * Get the cardinality values for a DB/Collection.
     *
     * @param string $storeName the database name to use
     * @param string $collName  the collection in the database
     * @param string $qName     either the qname to get the values for or empty for all cardinality values
     *
     * @return ($qName is null ? array<string, int> : array<string, int>|int) if no qname is specified then returns an array of cardinality options,
     *                                                                        otherwise returns the cardinality value for the given qname
     */
    public function getCardinality(string $storeName, string $collName, ?string $qName = null)
    {
        // If no qname specified the return all cardinality rules for this db/collection.
        if (empty($qName)) {
            return $this->cardinality[$storeName][$collName];
        }

        return $this->cardinality[$storeName][$collName][$qName] ?? -1;
    }

    /**
     * Returns a boolean reflecting whether or not the database and collection are defined in the config.
     */
    public function isPodWithinStore(string $storeName, string $pod): bool
    {
        return isset($this->podConnections[$storeName][$pod]);
    }

    /**
     * Returns an array of collection configurations for the supplied database name.
     */
    public function getPods(string $storeName): array
    {
        return isset($this->podConnections[$storeName]) ? array_keys($this->podConnections[$storeName]) : [];
    }

    /**
     * Returns the name of the data source for the request pod.  This may be the default for the store or the pod may
     * have overridden it in the config.
     *
     * @throws ConfigException
     */
    public function getDataSourceForPod(string $storeName, string $podName): string
    {
        if (isset($this->podConnections[$storeName][$podName])) {
            return $this->podConnections[$storeName][$podName];
        }

        throw new ConfigException(sprintf("'%s' not configured for store '%s'", $podName, $storeName));
    }

    /**
     * Returns a replica set name for the database, if one has been defined.
     */
    public function getReplicaSetName(string $datasource): ?string
    {
        if (empty($this->dataSources[$datasource])) {
            throw new ConfigException(sprintf("Data source '%s' not in configuration", $datasource));
        }

        if (!empty($this->dataSources[$datasource]['replicaSet'])) {
            return $this->dataSources[$datasource]['replicaSet'];
        }

        if (strpos($this->dataSources[$datasource]['connection'], 'replicaSet=') !== false) {
            $query = parse_url($this->dataSources[$datasource]['connection'], PHP_URL_QUERY);
            $params = [];
            if (is_string($query)) {
                parse_str($query, $params);
            }
            if (isset($params['replicaSet']) && is_string($params['replicaSet']) && ($params['replicaSet'] !== '' && $params['replicaSet'] !== '0')) {
                return $params['replicaSet'];
            }
        }

        return null;
    }

    /**
     * Returns a boolean reflecting whether or not a replica set has been defined for the supplied database name.
     */
    public function isReplicaSet(string $datasource): bool
    {
        return $this->getReplicaSetName($datasource) !== null;
    }

    public function getDefaultDataSourceForStore(string $storeName): ?string
    {
        return $this->dbConfig[$storeName]['data_source'] ?? null;
    }

    /**
     * Return the view specification document for the supplied id, if it exists.
     */
    public function getViewSpecification(string $storeName, string $vid): ?array
    {
        if (isset($this->viewSpecs[$storeName][$vid])) {
            return $this->viewSpecs[$storeName][$vid];
        }

        return null;
    }

    /**
     * Returns the search document specification for the supplied id, if it exists.
     */
    public function getSearchDocumentSpecification(string $storeName, string $sid): ?array
    {
        return $this->searchDocSpecs[$storeName][$sid] ?? null;
    }

    /**
     * Returns an array of all search document specifications, or specification ids.
     *
     * @param string|null $type             When supplied, will only return search document specifications that are triggered by this rdf:type
     * @param bool        $justReturnSpecId default is false. If true will only return an array of specification _id's, otherwise returns the array of specification documents
     */
    public function getSearchDocumentSpecifications(string $storeName, ?string $type = null, bool $justReturnSpecId = false): array
    {
        if (empty($this->searchDocSpecs[$storeName])) {
            return [];
        }

        $specs = [];

        if (empty($type)) {
            if ($justReturnSpecId) {
                $specIds = [];
                foreach ($this->searchDocSpecs[$storeName] as $specId => $spec) {
                    $specIds[] = $specId;
                }

                return $specIds;
            }

            return $this->searchDocSpecs[$storeName];
        }

        $labeller = $this->getLabeller();
        $typeAsUri = $labeller->uri_to_alias($type);
        $typeAsQName = $labeller->qname_to_alias($type);

        foreach ($this->searchDocSpecs[$storeName] as $spec) {
            if (is_array($spec[_ID_TYPE])) {
                if (in_array($typeAsUri, $spec[_ID_TYPE]) || in_array($typeAsQName, $spec[_ID_TYPE])) {
                    $specs[] = $justReturnSpecId ? $spec[_ID_KEY] : $spec;
                }
            } elseif ($spec[_ID_TYPE] == $typeAsUri || $spec[_ID_TYPE] == $typeAsQName) {
                $specs[] = $justReturnSpecId ? $spec[_ID_KEY] : $spec;
            }
        }

        return $specs;
    }

    /**
     * Returns the requested table specification, if it exists.
     */
    public function getTableSpecification(string $storeName, string $tid): ?array
    {
        return $this->tableSpecs[$storeName][$tid] ?? null;
    }

    /**
     * Returns all defined table specifications.
     *
     * @codeCoverageIgnore
     */
    public function getTableSpecifications(string $storeName): array
    {
        return $this->tableSpecs[$storeName] ?? [];
    }

    /**
     * Returns all defined view specification.
     *
     * @codeCoverageIgnore
     */
    public function getViewSpecifications(string $storeName): array
    {
        return $this->viewSpecs[$storeName] ?? [];
    }

    /**
     * This method returns a unique list of every rdf type configured in a specifications ['type'] restriction.
     *
     * @return string[] array of types
     */
    public function getAllTypesInSpecifications(string $storeName): array
    {
        $viewTypes = $this->getTypesInViewSpecifications($storeName);
        $tableTypes = $this->getTypesInTableSpecifications($storeName);
        $searchTypes = $this->getTypesInSearchSpecifications($storeName);
        $types = array_unique(array_merge($viewTypes, $tableTypes, $searchTypes));

        return array_values($types);
    }

    /**
     * Returns a unique list of every rdf type configured in the view spec ['type'] restriction.
     *
     * @return string[]
     */
    public function getTypesInViewSpecifications(string $storeName, ?string $pod = null): array
    {
        return array_values(array_unique($this->getSpecificationTypes($this->getViewSpecifications($storeName), $pod)));
    }

    /**
     * Returns a unique list of every rdf type configured in the table spec ['type'] restriction.
     *
     * @return string[]
     */
    public function getTypesInTableSpecifications(string $storeName, ?string $pod = null): array
    {
        return array_values(array_unique($this->getSpecificationTypes($this->getTableSpecifications($storeName), $pod)));
    }

    /**
     * Returns a unique list of every rdf type configured in the search doc spec ['type'] restriction.
     *
     * @return string[]
     */
    public function getTypesInSearchSpecifications(string $storeName, ?string $pod = null): array
    {
        return array_values(array_unique($this->getSpecificationTypes($this->getSearchDocumentSpecifications($storeName), $pod)));
    }

    /**
     * Returns an array of database names.
     *
     * @return string[]
     */
    public function getDbs(): array
    {
        return array_keys($this->dbConfig);
    }

    /**
     * Returns an array of defined namespaces.
     *
     * @return array<string, string> An associative array of namespaces, keyed by prefix
     */
    public function getNamespaces(): array
    {
        return $this->ns;
    }

    /**
     * Getter for transaction log connection config.
     */
    public function getTransactionLogConfig(): array
    {
        return $this->tConfig;
    }

    public function getSearchProviderClassName(string $storeName): ?string
    {
        return $this->searchProviderClassName[$storeName] ?? null;
    }

    /**
     * @throws ConfigException
     */
    public function getDatabase(string $storeName, ?string $dataSource = null, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Database
    {
        if (!isset($this->dbConfig[$storeName])) {
            throw new ConfigException(sprintf("Store name '%s' not in configuration", $storeName));
        }

        if (!$dataSource) {
            $dataSource = $this->dbConfig[$storeName]['data_source'];
        }

        $client = $this->getConnectionForDataSource($dataSource);

        return $client->selectDatabase($this->dbConfig[$storeName]['database'], [
            'readPreference' => new ReadPreference($readPreference),
        ]);
    }

    /**
     * @throws ConfigException
     */
    public function getCollectionForCBD(string $storeName, string $podName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        if (isset($this->podConnections[$storeName][$podName])) {
            return $this->getMongoCollection(
                $this->getDatabase($storeName, $this->podConnections[$storeName][$podName], $readPreference),
                $podName
            );
        }

        throw new ConfigException(sprintf("Collection name '%s' not in configuration for store '%s'", $podName, $storeName));
    }

    /**
     * @throws ConfigException
     */
    public function getCollectionForView(string $storeName, string $viewId, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        if (!isset($this->viewSpecs[$storeName][$viewId])) {
            throw new ConfigException(sprintf("View id '%s' not in configuration for store '%s'", $viewId, $storeName));
        }

        $dataSource = $this->viewSpecs[$storeName][$viewId]['to_data_source'] ?? null;

        return $this->getMongoCollection(
            $this->getDatabase($storeName, $dataSource, $readPreference),
            VIEWS_COLLECTION
        );
    }

    /**
     * @throws ConfigException
     */
    public function getCollectionForSearchDocument(string $storeName, string $searchDocumentId, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        if (!isset($this->searchDocSpecs[$storeName][$searchDocumentId])) {
            throw new ConfigException(sprintf("Search document id '%s' not in configuration for store '%s'", $searchDocumentId, $storeName));
        }

        $dataSource = $this->searchDocSpecs[$storeName][$searchDocumentId]['to_data_source'] ?? null;

        return $this->getMongoCollection(
            $this->getDatabase($storeName, $dataSource, $readPreference),
            SEARCH_INDEX_COLLECTION
        );
    }

    /**
     * @throws ConfigException
     */
    public function getCollectionForTable(string $storeName, string $tableId, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        if (!isset($this->tableSpecs[$storeName][$tableId])) {
            throw new ConfigException(sprintf("Table id '%s' not in configuration for store '%s'", $tableId, $storeName));
        }

        $dataSource = $this->tableSpecs[$storeName][$tableId]['to_data_source'] ?? null;

        return $this->getMongoCollection(
            $this->getDatabase($storeName, $dataSource, $readPreference),
            TABLE_ROWS_COLLECTION
        );
    }

    /**
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForTables(string $storeName, array $tables = [], string $readPreference = ReadPreference::PRIMARY_PREFERRED): array
    {
        if (!isset($this->tableSpecs[$storeName])) {
            return [];
        }

        if ($tables === []) {
            $tables = array_keys($this->tableSpecs[$storeName]);
        }

        $dataSources = [];
        foreach ($tables as $table) {
            if (!isset($this->tableSpecs[$storeName][$table])) {
                throw new ConfigException(sprintf("Table id '%s' not in configuration for store '%s'", $table, $storeName));
            }

            $dataSources[] = $this->tableSpecs[$storeName][$table]['to_data_source'] ?? null;
        }

        $collections = [];
        foreach (array_unique($dataSources) as $dataSource) {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                TABLE_ROWS_COLLECTION
            );
        }

        return $collections;
    }

    /**
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForViews(string $storeName, array $views = [], string $readPreference = ReadPreference::PRIMARY_PREFERRED): array
    {
        if (!isset($this->viewSpecs[$storeName])) {
            return [];
        }

        if ($views === []) {
            $views = array_keys($this->viewSpecs[$storeName]);
        }

        $dataSources = [];
        foreach ($views as $view) {
            if (!isset($this->viewSpecs[$storeName][$view])) {
                throw new ConfigException(sprintf("View id '%s' not in configuration for store '%s'", $view, $storeName));
            }

            $dataSources[] = $this->viewSpecs[$storeName][$view]['to_data_source'] ?? null;
        }

        $collections = [];
        foreach (array_unique($dataSources) as $dataSource) {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                VIEWS_COLLECTION
            );
        }

        return $collections;
    }

    /**
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForSearch(string $storeName, array $searchSpecIds = [], string $readPreference = ReadPreference::PRIMARY_PREFERRED): array
    {
        if (!isset($this->searchDocSpecs[$storeName])) {
            return [];
        }

        if ($searchSpecIds === []) {
            $searchSpecIds = array_keys($this->searchDocSpecs[$storeName]);
        }

        $dataSources = [];
        foreach ($searchSpecIds as $searchSpec) {
            if (!isset($this->searchDocSpecs[$storeName][$searchSpec])) {
                throw new ConfigException(sprintf("Search document spec id '%s' not in configuration for store '%s'", $searchSpec, $storeName));
            }

            $dataSources[] = $this->searchDocSpecs[$storeName][$searchSpec]['to_data_source'] ?? null;
        }

        $collections = [];
        foreach (array_unique($dataSources) as $dataSource) {
            $collections[] = $this->getMongoCollection(
                $this->getDatabase($storeName, $dataSource, $readPreference),
                SEARCH_INDEX_COLLECTION
            );
        }

        return $collections;
    }

    public function getCollectionForTTLCache(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            TTL_CACHE_COLLECTION
        );
    }

    public function getCollectionForLocks(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            LOCKS_COLLECTION
        );
    }

    public function getCollectionForManualRollbackAudit(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            AUDIT_MANUAL_ROLLBACKS_COLLECTION
        );
    }

    public function getCollectionForJobGroups(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection
    {
        return $this->getMongoCollection(
            $this->getDatabase($storeName, $this->dbConfig[$storeName]['data_source'], $readPreference),
            OPERATION_GROUPS_COLLECTION
        );
    }

    /**
     * @throws ConfigException
     */
    public function getTransactionLogDatabase(string $readPreference = ReadPreference::PRIMARY_PREFERRED): Database
    {
        $client = $this->getConnectionForDataSource($this->tConfig['data_source']);
        $db = $client->selectDatabase($this->tConfig['database']);

        return $db->withOptions(['readPreference' => new ReadPreference($readPreference)]);
    }

    public static function getDiscoverQueueName(): string
    {
        return self::getQueueName(TRIPOD_DISCOVER_QUEUE, 'discover');
    }

    public static function getApplyQueueName(): string
    {
        return self::getQueueName(TRIPOD_APPLY_QUEUE, 'apply');
    }

    public static function getEnsureIndexesQueueName(): string
    {
        return self::getQueueName(TRIPOD_ENSURE_INDEXES_QUEUE, 'ensureindexes');
    }

    public static function getResqueServer(): string
    {
        $resqueServer = self::getenv(RESQUE_SERVER, '');
        if (empty($resqueServer)) {
            $resqueServer = self::getenv(MONGO_TRIPOD_RESQUE_SERVER, '');
            if (!empty($resqueServer)) {
                self::getLogger()->notice('Use of MONGO_TRIPOD_RESQUE_SERVER is deprecated - use RESQUE_SERVER instead');
            }
        }

        if (empty($resqueServer)) {
            self::getLogger()->warning('RESQUE_SERVER is missing from environment - using localhost:6379 instead');
            $resqueServer = 'localhost:6379';
        }

        return $resqueServer;
    }

    public static function getLogger(): LoggerInterface
    {
        return DriverBase::getLogger();
    }

    /**
     * Sets the Tripod config.
     */
    public static function deserialize(array $config): IConfigInstance
    {
        if (isset($config['class'], $config['config'])) {
            $config = $config['config'];
        }

        $instance = new self();
        $instance->loadConfig($config);

        return $instance;
    }

    /**
     * Serializes the config into an array that can be passed to jobs, etc.
     */
    public function serialize(): array
    {
        if ($this->config === null) {
            throw new ConfigException('Config has not been loaded');
        }

        return $this->config;
    }

    /**
     * Return the maximum batch size for async operations.
     *
     * @param string $operation Async operation, e.g. OP_TABLES, OP_VIEWS
     */
    public function getBatchSize(string $operation): int
    {
        return $this->batchSize[$operation] ?? 1;
    }

    /**
     * Used to load the config from self::config when new instance is generated.
     *
     * @param array<string, mixed> $config
     *
     * @throws ConfigException
     */
    protected function loadConfig(array $config): void
    {
        $this->config = $config;
        if (isset($config['namespaces'])) {
            $this->ns = $config['namespaces'];
        }

        $this->defaultContext = $this->getMandatoryKey('defaultContext', $config);

        foreach ($this->getMandatoryKey('data_sources', $config) as $source => $c) {
            if (!isset($c['type'])) {
                throw new ConfigException("No 'type' set for data source " . $source);
            }

            if (!isset($c['connection'])) {
                throw new ConfigException('No connection information set for data source ' . $source);
            }

            $this->dataSources[$source] = $c;
        }

        $transactionConfig = $this->getMandatoryKey('transaction_log', $config);
        $this->tConfig['data_source'] = $this->getMandatoryKey('data_source', $transactionConfig, 'transaction_log');
        if (!isset($this->dataSources[$this->tConfig['data_source']])) {
            throw new ConfigException('Transaction log data source, ' . $this->tConfig['data_source'] . ', was not defined');
        }

        $this->tConfig['database'] = $this->getMandatoryKey('database', $transactionConfig, 'transaction_log');
        $this->tConfig['collection'] = $this->getMandatoryKey('collection', $transactionConfig, 'transaction_log');

        // A 'pod' corresponds to a logical database
        $this->databases = $this->getMandatoryKey('stores', $config);
        foreach ($this->databases as $storeName => $storeConfig) {
            $this->dbConfig[$storeName] = ['data_source' => $this->getMandatoryKey('data_source', $storeConfig)];
            if (isset($storeConfig['database']) && !empty($storeConfig['database'])) {
                $this->dbConfig[$storeName]['database'] = $storeConfig['database'];
            } else {
                $this->dbConfig[$storeName]['database'] = $storeName;
            }

            $this->cardinality[$storeName] = [];
            $this->indexes[$storeName] = [];
            $this->podConnections[$storeName] = [];
            if (isset($storeConfig['pods'])) {
                foreach ($storeConfig['pods'] as $podName => $podConfig) {
                    $dataSource = ($podConfig['data_source'] ?? $storeConfig['data_source']);
                    $this->podConnections[$storeName][$podName] = $dataSource;

                    // Set cardinality, also checking against defined namespaces
                    if (isset($podConfig['cardinality'])) {
                        // Test that the namespace exists for each cardinality rule defined
                        $cardinality = $podConfig['cardinality'];
                        foreach ($cardinality as $qname => $cardinalityValue) {
                            $namespaces = explode(':', $qname);
                            // just grab the first element
                            $namespace = array_shift($namespaces);

                            if (isset($this->ns[$namespace])) {
                                $this->cardinality[$storeName][$podName][] = $cardinality;
                            } else {
                                throw new ConfigException(sprintf("Cardinality '%s' does not have the namespace defined", $qname));
                            }
                        }
                    } else {
                        $this->cardinality[$storeName][$podName] = [];
                    }

                    $this->cardinality[$storeName][$podName] = $podConfig['cardinality'] ?? [];

                    // Ensure indexes are legal
                    if (isset($podConfig['indexes'])) {
                        $this->indexes[$storeName][$podName] = [];

                        foreach ($podConfig['indexes'] as $indexName => $indexFields) {
                            $this->indexes[$storeName][$podName][$indexName] = $indexFields;

                            $indexKeys = array_keys($indexFields);
                            if (is_numeric($indexKeys[0])) {
                                // New format config - two arrays, where second is index options (e.g. unique=>true, sparse=>true)
                                $cardinalityIndexFields = $indexFields[0];
                            } else {
                                // Standard format config - single array
                                $cardinalityIndexFields = $indexFields;
                            }

                            // check no more than 1 indexField is an array to ensure Mongo will be able to create compound indexes
                            if (count($cardinalityIndexFields) > 1) {
                                $fieldsThatAreArrays = 0;
                                foreach ($cardinalityIndexFields as $field => $fieldVal) {
                                    $cardinalityField = str_replace('.value', '', $field);
                                    if (
                                        !isset($this->cardinality[$storeName][$podName][$cardinalityField])
                                        || $this->cardinality[$storeName][$podName][$cardinalityField] != 1
                                    ) {
                                        $fieldsThatAreArrays++;
                                    }

                                    if ($fieldsThatAreArrays > 1) {
                                        throw new ConfigException(sprintf('Compound index %s has more than one field with cardinality > 1 - mongo will not be able to build this index', $indexName));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (isset($storeConfig['batch_sizes'])) {
                foreach ([OP_TABLES, OP_SEARCH, OP_VIEWS] as $op) {
                    if (isset($storeConfig['batch_sizes'][$op]) && is_numeric($storeConfig['batch_sizes'][$op])) {
                        $this->batchSize[$op] = intval($storeConfig['batch_sizes'][$op]);
                    }
                }
            }

            $searchConfig = $storeConfig['search_config'] ?? [];
            $this->searchDocSpecs[$storeName] = [];
            if (!empty($searchConfig)) {
                $this->searchProviderClassName[$storeName] = ltrim($this->getMandatoryKey('search_provider', $searchConfig, 'search'), '\\');
                // Load search doc specs if search_config is set
                $searchDocSpecs = $this->getMandatoryKey('search_specifications', $searchConfig, 'search');
                foreach ($searchDocSpecs as $spec) {
                    if (!isset($spec[_ID_KEY])) {
                        throw new ConfigException('Search document spec does not contain ' . _ID_KEY);
                    }

                    if (!isset($spec['from']) || !in_array($spec['from'], $this->getPods($storeName))) {
                        throw new ConfigException("'" . $spec[_ID_KEY] . "[\"from\"]' property not set or references an undefined pod");
                    }

                    if (!isset($spec['filter'])) {
                        throw new ConfigException("'" . $spec[_ID_KEY] . "[\"filter\"]' property not set");
                    }

                    if (!isset($spec['fields']) && !isset($spec['joins'])) {
                        throw new ConfigException("'" . $spec[_ID_KEY] . "' contains no 'fields' or 'joins' properties");
                    }

                    if ($this->searchProviderClassName[$storeName] == SEARCH_PROVIDER_MONGO) {
                        if (isset($spec['to_data_source'])) {
                            if (!isset($this->dataSources[$spec['to_data_source']])) {
                                throw new ConfigException("'" . $spec[_ID_KEY] . "[\"to_data_source\"]' property references an undefined data source");
                            }
                        } else {
                            $spec['to_data_source'] = $storeConfig['data_source'];
                        }
                    }

                    $this->searchDocSpecs[$storeName][$spec[_ID_KEY]] = $spec;
                }
            }

            // Load view specs
            $viewSpecs = $storeConfig['view_specifications'] ?? [];
            $this->viewSpecs[$storeName] = [];
            foreach ($viewSpecs as $spec) {
                if (!isset($spec[_ID_KEY])) {
                    throw new ConfigException('View spec does not contain ' . _ID_KEY);
                }

                if (!isset($spec['from']) || !in_array($spec['from'], $this->getPods($storeName))) {
                    throw new ConfigException("'" . $spec[_ID_KEY] . "[\"from\"]' property not set or references an undefined pod");
                }

                if (!isset($spec['joins'])) {
                    throw new ConfigException('Could not find any joins in view specification - usecase better served with select()');
                }

                $this->ifCountExistsWithoutTTLThrowException($spec);
                if (isset($spec['to_data_source'])) {
                    if (!isset($this->dataSources[$spec['to_data_source']])) {
                        throw new ConfigException("'" . $spec[_ID_KEY] . "[\"to_data_source\"]' property references an undefined data source");
                    }
                } else {
                    $spec['to_data_source'] = $storeConfig['data_source'];
                }

                $this->viewSpecs[$storeName][$spec[_ID_KEY]] = $spec;
            }

            // Load table specs
            $tableSpecs = $storeConfig['table_specifications'] ?? [];
            $this->tableSpecs[$storeName] = [];
            foreach ($tableSpecs as $spec) {
                $this->validateTableSpec($spec);

                if (isset($spec['to_data_source'])) {
                    if (!isset($this->dataSources[$spec['to_data_source']])) {
                        throw new ConfigException("'" . $spec[_ID_KEY] . "[\"to_data_source\"]' property references an undefined data source");
                    }
                } else {
                    $spec['to_data_source'] = $storeConfig['data_source'];
                }

                $this->tableSpecs[$storeName][$spec[_ID_KEY]] = $spec;
            }
        }
    }

    /**
     * @param array<string, mixed> $spec
     *
     * @throws ConfigException
     */
    protected function validateTableSpecPart(array $spec, int $depth = 0): void
    {
        $validationLevel = $this->getValidationLevel();
        if (!isset($spec['fields']) && !isset($spec['joins']) && !isset($spec['counts']) && !isset($spec['computed_fields'])) {
            throw new ConfigException('Table spec part does not contain fields, joins, counts, or computed_fields');
        }

        if (isset($spec['fields'])) {
            foreach ($spec['fields'] as $field) {
                if (!isset($field['fieldName'])) {
                    throw new ConfigException('Field spec does not contain fieldName');
                }

                if (isset($field['predicates'])) {
                    if ($validationLevel === self::VALIDATE_MAX) {
                        foreach ($field['predicates'] as $p) {
                            // If predicates is an array we've got modifiers
                            if (is_array($p)) {
                                /*
                                 * checkModifierFunctions will check if each predicate modifier is valid - it will
                                 * check recursively through the predicate
                                 */
                                $this->checkModifierFunctions($p, Tables::$predicateModifiers);
                            }
                        }
                    }
                }
                // fields can either have predicates or values
                elseif ((!isset($field['value'])) || empty($field['value'])) {
                    throw new ConfigException('Field spec does not contain predicates or value');
                }
            }
        }

        if (isset($spec['counts'])) {
            foreach ($spec['counts'] as $count) {
                if (!isset($count['fieldName'])) {
                    throw new ConfigException('Count spec does not contain fieldName');
                }

                if (isset($count['property'])) {
                    if (!is_string($count['property'])) {
                        throw new ConfigException('Count spec property was not a string');
                    }
                } else {
                    throw new ConfigException('Count spec does not contain property');
                }
            }
        }

        if (isset($spec['computed_fields'])) {
            if ($depth > 0) {
                throw new ConfigException("Table spec can only contain 'computed_fields' at the base level");
            }

            $validComputingFieldFunctions = Tables::$computedFieldFunctions;
            if ($validationLevel === self::VALIDATE_MAX) {
                $availableFields = $this->getFieldNamesInSpec($spec);
                $availableFields = array_map(fn (string $field): string => '$' . $field, $availableFields);
            }

            foreach ($spec['computed_fields'] as $field) {
                if (!isset($field['fieldName'])) {
                    throw new ConfigException('Computed field spec does not contain fieldName');
                }

                if (!isset($field['value'])) {
                    throw new ConfigException('Computed field spec does not contain value');
                }

                if (!is_array($field['value'])) {
                    throw new ConfigException('Compute field value does not contain computed field spec');
                }

                $functions = array_intersect(array_keys($field['value']), $validComputingFieldFunctions);

                if ($functions === []) {
                    throw new ConfigException('Computed field spec does not contain valid function');
                }

                if (count($functions) > 1) {
                    throw new ConfigException('Computed field spec contains more than one function');
                }

                if ($validationLevel === self::VALIDATE_MAX && isset($availableFields)) {
                    $this->validateComputedFieldSpec($functions[0], $field['value'], $availableFields);
                }
            }
        }

        if (isset($spec['joins'])) {
            $nextLevel = ($depth + 1);
            foreach ($spec['joins'] as $join) {
                $this->validateTableSpecPart($join, $nextLevel);
            }
        }
    }

    /**
     * @param string[]             $availableFields
     * @param array<string, mixed> $spec
     */
    protected function validateComputedFieldSpec(string $type, array $spec, array $availableFields): void
    {
        switch ($type) {
            case 'conditional':
                $this->validateComputedConditionalSpec($spec[$type], $availableFields);

                break;

            case 'replace':
                $this->validateComputedReplaceSpec($spec[$type], $availableFields);

                break;

            case 'arithmetic':
                $this->validateComputedArithmeticSpec($spec[$type], $availableFields);

                break;
        }
    }

    /**
     * @param array<string, mixed> $spec
     * @param string[]             $availableFields
     *
     * @throws ConfigException
     */
    protected function validateComputedConditionalSpec(array $spec, array $availableFields): void
    {
        if (!isset($spec['if'])) {
            throw new ConfigException("Computed conditional spec does not contain an 'if' value");
        }

        if (!isset($spec['then']) && !isset($spec['else'])) {
            throw new ConfigException('Computed conditional spec must contain a then or else value');
        }

        if (!is_array($spec['if'])) {
            throw new ConfigException("Computed conditional field spec 'if' value must be an array");
        }

        if (count($spec['if']) !== 1 && count($spec['if']) !== 3) {
            throw new ConfigException("Computed conditional field spec 'if' value array must have 1 or 3 values");
        }

        $this->validateSpecVariableReplacement($spec['if'][0], $availableFields);
        if (isset($spec['if'][1]) && !in_array($spec['if'][1], Tables::$conditionalOperators)) {
            throw new ConfigException("Invalid conditional operator '" . $spec['if'][1] . "' in conditional spec");
        }

        if (isset($spec['if'][2])) {
            $this->validateSpecVariableReplacement($spec['if'][2], $availableFields);
        }

        if (isset($spec['then'])) {
            if (is_string($spec['then'])) {
                $this->validateSpecVariableReplacement($spec['then'], $availableFields);
            } elseif (is_array($spec['then'])) {
                $functions = array_intersect_key(array_keys($spec['then']), Tables::$computedFieldFunctions);

                switch (count($functions)) {
                    case 0:
                        break;

                    case 1:
                        $this->validateComputedFieldSpec($functions[0], $spec['then'], $availableFields);

                        break;

                    default:
                        throw new ConfigException("Computed conditional field 'then' value has more than one function");
                }
            }
        }

        if (isset($spec['else'])) {
            if (is_string($spec['else'])) {
                $this->validateSpecVariableReplacement($spec['else'], $availableFields);
            } elseif (is_array($spec['else'])) {
                $functions = array_intersect_key(array_keys($spec['else']), Tables::$computedFieldFunctions);

                switch (count($functions)) {
                    case 0:
                        break;

                    case 1:
                        $this->validateComputedFieldSpec($functions[0], $spec['else'], $availableFields);

                        break;

                    default:
                        throw new ConfigException("Computed conditional field 'else' value has more than one function");
                }
            }
        }
    }

    /**
     * @param array|string $value
     * @param string[]     $availableFields
     *
     * @throws ConfigException
     */
    protected function validateSpecVariableReplacement($value, array $availableFields): void
    {
        if (is_string($value)) {
            if (strpos($value, '$') === 0 && !in_array($value, $availableFields)) {
                throw new ConfigException(sprintf("Computed spec variable '%s' is not defined in table spec", $value));
            }
        } elseif (is_array($value)) {
            foreach ($value as $v) {
                $this->validateSpecVariableReplacement($v, $availableFields);
            }
        }
    }

    /**
     * @param array<string, mixed> $spec
     * @param string[]             $availableFields
     *
     * @throws ConfigException
     */
    protected function validateComputedReplaceSpec(array $spec, array $availableFields): void
    {
        if (!isset($spec['search'])) {
            throw new ConfigException("Computed replace spec does not contain 'search' value");
        }

        $this->validateSpecVariableReplacement($spec['search'], $availableFields);
        if (!isset($spec['replace'])) {
            throw new ConfigException("Computed replace spec does not contain 'replace' value");
        }

        $this->validateSpecVariableReplacement($spec['replace'], $availableFields);
        if (!isset($spec['subject'])) {
            throw new ConfigException("Computed replace spec does not contain 'subject' value");
        }

        $this->validateSpecVariableReplacement($spec['subject'], $availableFields);
    }

    /**
     * @param array<int, mixed> $spec
     * @param string[]          $availableFields
     *
     * @throws ConfigException
     */
    protected function validateComputedArithmeticSpec(array $spec, array $availableFields): void
    {
        if (count($spec) !== 3) {
            throw new ConfigException('Computed arithmetic spec must contain 3 values');
        }

        if (is_array($spec[0])) {
            if (count(array_keys($spec[0])) === 1 && count(array_intersect(array_keys($spec[0]), Tables::$computedFieldFunctions)) === 1) {
                $function = array_keys($spec[0]);
                $this->validateComputedFieldSpec($function[0], $spec[0], $availableFields);
            } else {
                $this->validateComputedArithmeticSpec($spec[0], $availableFields);
            }
        } else {
            $this->validateSpecVariableReplacement($spec[0], $availableFields);
        }

        if (is_array($spec[2])) {
            if (count(array_keys($spec[2])) === 1 && count(array_intersect(array_keys($spec[2]), Tables::$computedFieldFunctions)) === 1) {
                $function = array_keys($spec[2]);
                $this->validateComputedFieldSpec($function[0], $spec[2], $availableFields);
            } else {
                $this->validateComputedArithmeticSpec($spec[2], $availableFields);
            }
        } else {
            $this->validateSpecVariableReplacement($spec[2], $availableFields);
        }

        if (!in_array($spec[1], Tables::$arithmeticOperators)) {
            throw new ConfigException("Invalid arithmetic operator '" . $spec[1] . "' in computed arithmetic spec");
        }
    }

    /**
     * @param array<string, mixed> $spec
     */
    protected function getFieldNamesInSpec(array $spec): array
    {
        $fieldNames = [];
        if (isset($spec['fields'])) {
            foreach ($spec['fields'] as $field) {
                if (isset($field['fieldName'])) {
                    $fieldNames[] = $field['fieldName'];
                }
            }
        }

        if (isset($spec['counts'])) {
            foreach ($spec['counts'] as $count) {
                if (isset($count['fieldName'])) {
                    $fieldNames[] = $count['fieldName'];
                }
            }
        }

        if (isset($spec['computed_fields'])) {
            foreach ($spec['computed_fields'] as $field) {
                if (isset($field['fieldName'])) {
                    $fieldNames[] = $field['fieldName'];
                }
            }
        }

        if (isset($spec['joins'])) {
            foreach ($spec['joins'] as $join) {
                $fieldNames = array_merge($fieldNames, $this->getFieldNamesInSpec($join));
            }
        }

        return $fieldNames;
    }

    /**
     * Creates an associative array of all predicates/properties associated with all table and search document specifications.
     */
    protected function getDefinedPredicatesInSpecs(string $storename): array
    {
        $predicates = [];
        $specs = array_merge($this->getTableSpecifications($storename), $this->getSearchDocumentSpecifications($storename));
        foreach ($specs as $spec) {
            if (!isset($spec[_ID_KEY])) {
                continue;
            }

            $predicates[$spec[_ID_KEY]] = array_unique($this->getDefinedPredicatesInSpecBlock($spec));
        }

        return $predicates;
    }

    /**
     * Recursively crawls a configuration document array (or part of one) and returns any associated predicates/properties.
     *
     * @param array<string, mixed> $block
     */
    protected function getDefinedPredicatesInSpecBlock(array $block): array
    {
        $predicates = [];
        // If the spec has a "type" property, include rdf:type
        if (isset($block['type'])) {
            $predicates[] = $this->getLabeller()->uri_to_alias(RDF_TYPE);
        }

        if (isset($block['filter'])) {
            foreach ($block['filter'] as $filter) {
                if (isset($filter['condition'])) {
                    $predicates = array_merge($predicates, $this->getPredicatesFromFilterCondition($filter['condition']));
                }
            }
        }

        // Get the predicates out of the defined fields
        if (isset($block['fields'])) {
            foreach ($block['fields'] as $field) {
                if (isset($field['predicates'])) {
                    foreach ($field['predicates'] as $p) {
                        if (!empty($p)) {
                            // The actual predicate strings may be buried in predicate function blocks
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }

        // Loop through the joins and pass the objects back to this method
        if (isset($block['joins'])) {
            foreach ($block['joins'] as $predicate => $join) {
                // Joins are keyed on the predicate, so save that
                $predicates[] = $this->getLabeller()->uri_to_alias($predicate);
                $predicates = array_merge($predicates, $this->getDefinedPredicatesInSpecBlock($join));
            }
        }

        // Loop through the counts blocks
        if (isset($block['counts'])) {
            foreach ($block['counts'] as $property) {
                // counts use the redundant property 'property', which behaves exactly like a predicate and needs to be deprecated
                if (isset($property['property'])) {
                    $predicates[] = $this->getLabeller()->uri_to_alias($property['property']);
                }

                // This is here so we can easily deprecate 'property' in favor of 'predicates'
                if (isset($property['predicates'])) {
                    foreach ($property['predicates'] as $p) {
                        if (!empty($p)) {
                            // The actual predicate strings may be buried in predicate function blocks
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }

        // Loop through the indices: these should be more or less identical to 'fields'
        if (isset($block['indices'])) {
            foreach ($block['indices'] as $index) {
                if (isset($index['predicates'])) {
                    foreach ($index['predicates'] as $p) {
                        if (!empty($p)) {
                            $predicates = array_merge($predicates, $this->getPredicateAliasesFromPredicateProperty($p));
                        }
                    }
                }
            }
        }

        return $predicates;
    }

    /**
     * Rewrites any predicate uris to alias curies.
     *
     * @param array|string $predicate
     */
    protected function getPredicateAliasesFromPredicateProperty($predicate): array
    {
        $predicates = [];
        if (is_string($predicate) && ($predicate !== '' && $predicate !== '0')) {
            $predicates[] = $this->getLabeller()->uri_to_alias($predicate);
        } elseif (is_array($predicate)) {
            foreach ($this->getPredicatesFromPredicateFunctions($predicate) as $p) {
                $predicates[] = $this->getLabeller()->uri_to_alias($p);
            }
        }

        return $predicates;
    }

    /**
     * When given an array as input, will traverse any predicate functions and return the predicate strings.
     */
    protected function getPredicatesFromPredicateFunctions(array $array): array
    {
        $predicates = [];
        if (isset($array['predicates'])) {
            $predicates = $array['predicates'];
        } elseif ($array !== []) {
            $function = key($array);
            if (is_array($array[$function])) {
                $predicates = array_merge($predicates, $this->getPredicatesFromPredicateFunctions($array[$function]));
            }
        }

        return $predicates;
    }

    /**
     * Parses a specDocument's "filter" parameter for any predicates.
     *
     * @return string[]
     */
    protected function getPredicatesFromFilterCondition(array $filter): array
    {
        $predicates = [];
        $regex = '/(^|\b)(\w+\:\w+)\.(l|u)(\b|$)/';
        foreach ($filter as $key => $condition) {
            if (is_string($key)) {
                $numMatches = preg_match_all($regex, $key, $matches);
                for ($i = 0; $i < $numMatches; $i++) {
                    if (isset($matches[2][$i])) {
                        $predicates[] = $matches[2][$i];
                    }
                }
            }

            if (is_string($condition)) {
                $numMatches = preg_match_all($regex, $condition, $matches);
                for ($i = 0; $i < $numMatches; $i++) {
                    if (isset($matches[2][$i])) {
                        $predicates[] = $matches[2][$i];
                    }
                }
            } elseif (is_array($condition)) {
                array_merge($predicates, $this->getPredicatesFromFilterCondition($condition));
            }
        }

        return $predicates;
    }

    protected function getLabeller(): Labeller
    {
        if ($this->labeller == null) {
            $this->labeller = new Labeller();
        }

        return $this->labeller;
    }

    /**
     * @throws ConfigException
     * @throws ConnectionTimeoutException
     */
    protected function getConnectionForDataSource(string $dataSource): Client
    {
        if (!isset($this->dataSources[$dataSource])) {
            throw new ConfigException(sprintf("Data source '%s' not in configuration", $dataSource));
        }

        if (!isset($this->connections[$dataSource])) {
            $ds = $this->dataSources[$dataSource];
            $connectionString = $ds['connection'];
            $connectionOptions = [];

            if (!empty($ds['connectTimeoutMS']) || strpos($connectionString, 'connectTimeoutMS=') === false) {
                $connectionOptions['connectTimeoutMS'] = $ds['connectTimeoutMS'] ?? DEFAULT_MONGO_CONNECT_TIMEOUT_MS;
            }

            if (!empty($ds['replicaSet'])) {
                $connectionOptions['replicaSet'] = $ds['replicaSet'];
            }

            $retries = 1;
            $exception = null;

            do {
                try {
                    $this->connections[$dataSource] = $this->getMongoClient($connectionString, $connectionOptions);

                    break;
                } catch (ConnectionTimeoutException $e) {
                    self::getLogger()->error('ConnectionTimeoutException attempt ' . $retries . '. Retrying...:' . $e->getMessage());
                    sleep(1);
                    $retries++;
                    $exception = $e;
                }
            } while ($retries <= self::CONNECTION_RETRIES);

            if (!isset($this->connections[$dataSource]) && $exception !== null) {
                self::getLogger()->error('MongoConnectionException failed after ' . $retries . ' attempts (MAX:' . self::CONNECTION_RETRIES . '): ' . $exception->getMessage());

                throw $exception;
            }
        }

        return $this->connections[$dataSource];
    }

    /**
     * Create a Mongo Client - used for mocking.
     */
    protected function getMongoClient(string $connectionString, array $connectionOptions = []): Client
    {
        return new Client(
            $connectionString,
            $connectionOptions,
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
    }

    protected function getMongoCollection(Database $db, string $collectionName): Collection
    {
        return $db->selectCollection($collectionName);
    }

    // PRIVATE FUNCTIONS
    /**
     * Returns a unique list of every rdf type configured in the supplied specs' ['type'] restriction.
     *
     * @return string[]
     */
    private function getSpecificationTypes(array $specifications, ?string $podName = null): array
    {
        $types = [];
        foreach ($specifications as $spec) {
            if (!empty($podName) && $spec['from'] !== $podName) {
                continue;
                // skip this view spec if it isnt for the collection
            }

            if (is_array($spec[_ID_TYPE])) {
                $types = array_merge($spec[_ID_TYPE], $types);
            } else {
                $types[] = $spec[_ID_TYPE];
            }
        }

        return $types;
    }

    /**
     * If we have 'counts' in a view spec, a 'ttl' must be defined.
     * Note: this does not apply to tables or search docs.
     *
     * @param array<string, mixed> $spec
     *
     * @throws ConfigException
     */
    private function ifCountExistsWithoutTTLThrowException(array $spec): void
    {
        if (isset($spec['ttl'])) {
            return; // ttl exists
        }

        if (isset($spec['joins'])) {
            // recurse
            foreach ($spec['joins'] as $join) {
                $this->ifCountExistsWithoutTTLThrowException($join);
            }
        }

        if (isset($spec['counts'])) {
            throw new ConfigException('Aggregate function counts exists in spec, but no TTL defined');
        }
    }

    /**
     * Returns the value of the supplied key or throws an error, if missing.
     *
     * @param array<string, mixed> $a
     *
     * @return mixed
     *
     * @throws ConfigException
     */
    private function getMandatoryKey(string $key, array $a, string $configName = 'config')
    {
        if (!array_key_exists($key, $a)) {
            throw new ConfigException(sprintf('Mandatory config key [%s] is missing from %s', $key, $configName));
        }

        return $a[$key];
    }

    private static function getQueueName(string $envVar, string $type): string
    {
        $default = defined('APP_ENV') ? 'tripod::' . constant('APP_ENV') . ('::' . $type) : 'tripod::' . $type;

        return self::getenv($envVar, $default);
    }

    /**
     * @param false|string $default a fallback value, or false to require the environment variable to be set
     *
     * @throws ConfigException
     */
    private static function getenv(string $env, $default = false): string
    {
        $var = getenv($env);
        if ($var) {
            return $var;
        }

        if ($default !== false) {
            return $default;
        }

        throw new ConfigException('Missing value for environmental variable ' . $env);
    }
}
