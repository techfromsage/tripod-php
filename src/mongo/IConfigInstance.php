<?php

declare(strict_types=1);

namespace Tripod\Mongo;

use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\ReadPreference;
use Psr\Log\LoggerInterface;
use Tripod\Exceptions\ConfigException;
use Tripod\ISearchProvider;
use Tripod\ITripodConfigSerializer;

interface IConfigInstance extends ITripodConfigSerializer
{
    public function getMongoCursorTimeout(): int;

    /**
     * @param int $mongoCursorTimeout Timeout in ms
     */
    public function setMongoCursorTimeout(int $mongoCursorTimeout): void;

    /**
     * Returns an array of associated predicates in a table or search document specification
     * Note: will not return viewSpec predicates.
     *
     * @param string $storename Store name
     * @param string $specId    Composite spec id
     */
    public function getDefinedPredicatesInSpec(string $storename, string $specId): array;

    /**
     * Returns an alias curie of the default context (i.e. graph name).
     */
    public function getDefaultContextAlias(): string;

    /**
     * Returns a list of the configured indexes grouped by collection.
     *
     * @param string $storeName Store name
     */
    public function getIndexesGroupedByCollection(string $storeName): array;

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
    public function getCardinality(string $storeName, string $collName, ?string $qName = null);

    /**
     * Returns a boolean reflecting whether or not the database and collection are defined in the config.
     *
     * @param string $storeName Store name
     * @param string $pod       Pod name
     */
    public function isPodWithinStore(string $storeName, string $pod): bool;

    /**
     * Returns an array of collection configurations for the supplied database name.
     *
     * @param string $storeName Store name
     */
    public function getPods(string $storeName): array;

    /**
     * Returns the name of the data source for the request pod.  This may be the default for the store or the pod may
     * have overridden it in the config.
     *
     * @throws ConfigException
     */
    public function getDataSourceForPod(string $storeName, string $podName): string;

    /**
     * Return the view specification document for the supplied id, if it exists.
     *
     * @param string $storeName Store name
     * @param string $vid       View spec ID
     */
    public function getViewSpecification(string $storeName, string $vid): ?array;

    /**
     * Returns the search document specification for the supplied id, if it exists.
     *
     * @param string $storeName Store name
     * @param string $sid       Search document spec ID
     */
    public function getSearchDocumentSpecification(string $storeName, string $sid): ?array;

    /**
     * Returns an array of all search document specifications, or specification ids.
     *
     * @param string      $storeName        Store name
     * @param string|null $type             When supplied, will only return search document specifications that are triggered by this rdf:type
     * @param bool        $justReturnSpecId default is false. If true will only return an array of specification _id's, otherwise returns the array of specification documents
     */
    public function getSearchDocumentSpecifications(string $storeName, ?string $type = null, bool $justReturnSpecId = false): array;

    /**
     * Returns the requested table specification, if it exists.
     *
     * @param string $storeName Store name
     * @param string $tid       Table spec ID
     */
    public function getTableSpecification(string $storeName, string $tid): ?array;

    /**
     * Returns all defined table specifications.
     *
     * @param string $storeName Store name
     */
    public function getTableSpecifications(string $storeName): array;

    /**
     * Returns all defined view specification.
     *
     * @param string $storeName Store name
     */
    public function getViewSpecifications(string $storeName): array;

    /**
     * Returns a unique list of every rdf type configured in the view spec ['type'] restriction.
     *
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     */
    public function getTypesInViewSpecifications(string $storeName, ?string $pod = null): array;

    /**
     * Returns a unique list of every rdf type configured in the table spec ['type'] restriction.
     *
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     */
    public function getTypesInTableSpecifications(string $storeName, ?string $pod = null): array;

    /**
     * Returns a unique list of every rdf type configured in the search doc spec ['type'] restriction.
     *
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     */
    public function getTypesInSearchSpecifications(string $storeName, ?string $pod = null): array;

    /**
     * Returns a unique list of every rdf type configured in view, table, and search specifications.
     *
     * @param string $storeName Store name
     */
    public function getAllTypesInSpecifications(string $storeName): array;

    /**
     * Returns an array of database names.
     *
     * @return string[]
     */
    public function getDbs(): array;

    /**
     * Returns an array of defined namespaces.
     *
     * @return array<string, string>
     */
    public function getNamespaces(): array;

    /**
     * Getter for transaction log connection config.
     */
    public function getTransactionLogConfig(): array;

    /**
     * @param string $storeName Store name
     *
     * @return string|null the name of a class expected to implement ISearchProvider; callers must
     *                     validate with class_exists() since the value comes straight from config
     */
    public function getSearchProviderClassName(string $storeName): ?string;

    /**
     * @param string      $storeName      Store (database) name
     * @param string|null $dataSource     Database server identifier
     * @param string      $readPreference Mongo read preference
     *
     * @throws ConfigException
     */
    public function getDatabase(string $storeName, ?string $dataSource = null, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Database;

    /**
     * @param string $storeName      Store (database) name
     * @param string $podName        Pod (collection) name
     * @param string $readPreference Mongo read preference
     *
     * @throws ConfigException
     */
    public function getCollectionForCBD(string $storeName, string $podName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    /**
     * @param string $storeName      Store (database) name
     * @param string $viewId         View spec ID
     * @param string $readPreference Mongo read preference
     *
     * @throws ConfigException
     */
    public function getCollectionForView(string $storeName, string $viewId, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    /**
     * @param string $storeName        Store (database) name
     * @param string $searchDocumentId Search document spec ID
     * @param string $readPreference   Mongo read preference
     *
     * @throws ConfigException
     */
    public function getCollectionForSearchDocument(
        string $storeName,
        string $searchDocumentId,
        string $readPreference = ReadPreference::PRIMARY_PREFERRED
    ): Collection;

    /**
     * @param string $storeName      Store (database) name
     * @param string $tableId        Table spec ID
     * @param string $readPreference Mongo read preference
     *
     * @throws ConfigException
     */
    public function getCollectionForTable(string $storeName, string $tableId, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    /**
     * @param string $storeName      Store (database) name
     * @param array  $tables         Array of table spec IDs
     * @param string $readPreference Mongo read preference
     *
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForTables(
        string $storeName,
        array $tables = [],
        string $readPreference = ReadPreference::PRIMARY_PREFERRED
    ): array;

    /**
     * @param string $storeName      Store (database) name
     * @param array  $views          Array of view spec IDs
     * @param string $readPreference Mongo read preference
     *
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForViews(
        string $storeName,
        array $views = [],
        string $readPreference = ReadPreference::PRIMARY_PREFERRED
    ): array;

    /**
     * @param string $storeName      Store (database) name
     * @param array  $searchSpecIds  Array of search document spec IDs
     * @param string $readPreference Mongo read preference
     *
     * @return Collection[]
     *
     * @throws ConfigException
     */
    public function getCollectionsForSearch(
        string $storeName,
        array $searchSpecIds = [],
        string $readPreference = ReadPreference::PRIMARY_PREFERRED
    ): array;

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     */
    public function getCollectionForTTLCache(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    public function getCollectionForLocks(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     */
    public function getCollectionForManualRollbackAudit(
        string $storeName,
        string $readPreference = ReadPreference::PRIMARY_PREFERRED
    ): Collection;

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     */
    public function getCollectionForJobGroups(string $storeName, string $readPreference = ReadPreference::PRIMARY_PREFERRED): Collection;

    /**
     * @param string $readPreference Mongo read preference
     *
     * @throws ConfigException
     */
    public function getTransactionLogDatabase(string $readPreference = ReadPreference::PRIMARY_PREFERRED): Database;

    /**
     * Return the maximum batch size for async operations.
     *
     * @param string $operation Async operation, e.g. OP_TABLES, OP_VIEWS
     */
    public function getBatchSize(string $operation): int;

    public static function getDiscoverQueueName(): string;

    public static function getApplyQueueName(): string;

    public static function getEnsureIndexesQueueName(): string;

    public static function getResqueServer(): string;

    public static function getLogger(): LoggerInterface;
}
