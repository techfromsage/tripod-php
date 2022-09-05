<?php

namespace Tripod\Mongo;

use \MongoDB\Database;
use \MongoDB\Collection;
use \MongoDB\Driver\ReadPreference;

interface IConfigInstance extends \Tripod\ITripodConfigSerializer
{
    /**
     * @return int
     */
    public function getMongoCursorTimeout();

    /**
     * @param int $mongoCursorTimeout Timeout in ms
     */
    public function setMongoCursorTimeout($mongoCursorTimeout);

    /**
     * Returns an array of associated predicates in a table or search document specification
     * Note: will not return viewSpec predicates
     *
     * @param string $storename Store name
     * @param string $specId    Composite spec id
     * @return array
     */
    public function getDefinedPredicatesInSpec($storename, $specId);

    /**
     * Returns an alias curie of the default context (i.e. graph name)
     *
     * @return string
     */
    public function getDefaultContextAlias();

    /**
     * Returns a list of the configured indexes grouped by collection
     * @param string $storeName Store name
     * @return array
     */
    public function getIndexesGroupedByCollection($storeName);

    /**
     * Get the cardinality values for a DB/Collection.
     *
     * @param string $storeName The database name to use.
     * @param string $collName The collection in the database.
     * @param string $qName Either the qname to get the values for or empty for all cardinality values.
     * @return array|int If no qname is specified then returns an array of cardinality options,
     *                   otherwise returns the cardinality value for the given qname.
     */
    public function getCardinality($storeName, $collName, $qName = null);

    /**
     * Returns a boolean reflecting whether or not the database and collection are defined in the config
     * @param string $storeName Store name
     * @param string $pod       Pod name
     * @return bool
     */
    public function isPodWithinStore($storeName, $pod);

    /**
     * Returns an array of collection configurations for the supplied database name
     * @param string $storeName Store name
     * @return array
     */
    public function getPods($storeName);

    /**
     * Return the view specification document for the supplied id, if it exists
     * @param string $storeName Store name
     * @param string $vid       View spec ID
     * @return array|null
     */
    public function getViewSpecification($storeName, $vid);

    /**
     * Returns the search document specification for the supplied id, if it exists
     * @param string $storeName Store name
     * @param string $sid       Search document spec ID
     * @return array|null
     */
    public function getSearchDocumentSpecification($storeName, $sid);

    /**
     * Returns an array of all search document specifications, or specification ids
     *
     * @param string $storeName Store name
     * @param string|null $type When supplied, will only return search document specifications that are triggered by this rdf:type
     * @param bool $justReturnSpecId default is false. If true will only return an array of specification _id's, otherwise returns the array of specification documents
     * @return array
     */
    public function getSearchDocumentSpecifications($storeName, $type = null, $justReturnSpecId = false);

    /**
     * Returns the requested table specification, if it exists
     *
     * @param string $storeName Store name
     * @param string $tid       Table spec ID
     * @return array|null
     */
    public function getTableSpecification($storeName, $tid);

    /**
     * Returns all defined table specifications
     *
     * @param string $storeName Store name
     * @return array
     */
    public function getTableSpecifications($storeName);

    /**
     * Returns all defined view specification
     *
     * @param string $storeName Store name
     * @return array
     */
    public function getViewSpecifications($storeName);

    /**
     * Returns a unique list of every rdf type configured in the view spec ['type'] restriction
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     * @return array
     */
    public function getTypesInViewSpecifications($storeName, $pod = null);

    /**
     * Returns a unique list of every rdf type configured in the table spec ['type'] restriction
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     * @return array
     */
    public function getTypesInTableSpecifications($storeName, $pod = null);

    /**
     * Returns a unique list of every rdf type configured in the search doc spec ['type'] restriction
     * @param string      $storeName Store name
     * @param string|null $pod       Pod name
     * @return array
     */
    public function getTypesInSearchSpecifications($storeName, $pod = null);

    /**
     * Returns an array of database names
     *
     * @return array
     */
    public function getDbs();

    /**
     * Returns an array of defined namespaces
     * @return array
     */
    public function getNamespaces();

    /**
     * @param string $storeName Store name
     * @return string|null
     */
    public function getSearchProviderClassName($storeName);

    /**
     * @param string      $storeName      Store (database) name
     * @param string|null $dataSource     Database server identifier
     * @param string      $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Database
     */
    public function getDatabase($storeName, $dataSource = null, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param string $storeName      Store (database) name
     * @param string $podName        Pod (collection) name
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection
     */
    public function getCollectionForCBD($storeName, $podName, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param string $storeName      Store (database) name
     * @param string $viewId         View spec ID
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection
     */
    public function getCollectionForView($storeName, $viewId, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param string $storeName        Store (database) name
     * @param string $searchDocumentId Search document spec ID
     * @param string $readPreference   Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection
     */
    public function getCollectionForSearchDocument(
        $storeName,
        $searchDocumentId,
        $readPreference = ReadPreference::RP_PRIMARY_PREFERRED
    );

    /**
     * @param string $storeName      Store (database) name
     * @param string $tableId        Table spec ID
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection
     */
    public function getCollectionForTable($storeName, $tableId, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param string $storeName      Store (database) name
     * @param array  $tables         Array of table spec IDs
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection[]
     */
    public function getCollectionsForTables(
        $storeName,
        array $tables = [],
        $readPreference = ReadPreference::RP_PRIMARY_PREFERRED
    );

    /**
     * @param string $storeName      Store (database) name
     * @param array  $views          Array of view spec IDs
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection[]
     */
    public function getCollectionsForViews(
        $storeName,
        array $views = [],
        $readPreference = ReadPreference::RP_PRIMARY_PREFERRED
    );

    /**
     * @param string $storeName      Store (database) name
     * @param array  $searchSpecIds  Array of search document spec IDs
     * @param string $readPreference Mongo read preference
     * @throws \Tripod\Exceptions\ConfigException
     * @return Collection[]
     */
    public function getCollectionsForSearch(
        $storeName,
        array $searchSpecIds = [],
        $readPreference = ReadPreference::RP_PRIMARY_PREFERRED
    );

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     * @return Collection
     */
    public function getCollectionForTTLCache($storeName, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     * @return Collection
     */
    public function getCollectionForManualRollbackAudit(
        $storeName,
        $readPreference = ReadPreference::RP_PRIMARY_PREFERRED
    );

    /**
     * @param string $storeName      Store (database) name
     * @param string $readPreference Mongo read preference
     * @return Collection
     */
    public function getCollectionForJobGroups($storeName, $readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * @param $readPreference Mongo read preference
     * @return Database
     * @throws \Tripod\Exceptions\ConfigException
     */
    public function getTransactionLogDatabase($readPreference = ReadPreference::RP_PRIMARY_PREFERRED);

    /**
     * Return the maximum batch size for async operations
     *
     * @param string $operation Async operation, e.g. OP_TABLES, OP_VIEWS
     * @return integer
     */
    public function getBatchSize($operation);

    /**
     * @return string
     */
    public static function getDiscoverQueueName();

    /**
     * @return string
     */
    public static function getApplyQueueName();

    /**
     * @return string
     */
    public static function getEnsureIndexesQueueName();

    /**
     * @return string
     */
    public static function getResqueServer();

    /**
     * @return int|string
     */
    public static function getResqueDatabase();

    /**
     * @return \Psr\Log\LoggerInterface;
     */
    public static function getLogger();
}
