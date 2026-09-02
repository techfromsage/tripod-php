<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Exceptions\Exception;
use Tripod\Exceptions\SearchException;

interface IDriver
{
    /**
     * Equivalent to CONSTRUCT.
     *
     * @param array $filter            conditions to filter by
     * @param array $includeProperties only include these predicates, empty array means return all predicates
     */
    public function graph(array $filter, array $includeProperties = []): ExtendedGraph;

    /**
     * Return (DESCRIBE) the concise bound description of a resource.
     *
     * @param string      $resource uri resource you'd like to describe
     * @param string|null $context  string uri of the context, or named graph, you'd like to describe from
     */
    public function describeResource(string $resource, ?string $context = null): ExtendedGraph;

    /**
     * Return (DESCRIBE) the concise bound descriptions of a bunch of resources.
     *
     * @param array       $resources uris of resources you'd like to describe
     * @param string|null $context   string uri of the context, or named graph, you'd like to describe from
     */
    public function describeResources(array $resources, ?string $context = null): ExtendedGraph;

    /**
     * Get a view of a given type for a given resource.
     *
     * @param string|null $resource uri of the resource you'd like the view for
     * @param string      $viewType string type of view
     */
    public function getViewForResource(?string $resource, string $viewType): ExtendedGraph;

    /**
     * Get views for multiple resources in one graph.
     *
     * @param string[] $resources uris of resources you'd like to describe
     * @param string   $viewType  type of view
     */
    public function getViewForResources(array $resources, string $viewType): ExtendedGraph;

    /**
     * Get views based on a pattern-match $filter.
     *
     * @param array  $filter   pattern to match to select views
     * @param string $viewType type of view
     */
    public function getViews(array $filter, string $viewType): ExtendedGraph;

    /**
     * Returns the etag of a resource, useful for caching.
     */
    public function getETag(string $resource, ?string $context = null): string;

    /**
     * Select data in a tabular format.
     *
     * @param array $fields array of fields, in the same format as prescribed by MongoPHP
     */
    public function select(
        array $query,
        array $fields,
        ?array $sortBy = null,
        ?int $limit = null,
        ?int $offset = 0,
        ?string $context = null
    ): array;

    /**
     * Select data from a table.
     */
    public function getTableRows(
        string $tableType,
        array $filter = [],
        ?array $sortBy = [],
        ?int $offset = 0,
        ?int $limit = 10,
        array $options = []
    ): array;

    public function getDistinctTableColumnValues(string $tableType, string $fieldName, array $filter = []): array;

    /**
     * Get a count of resources matching the pattern in $query. Optionally group counts by specifying a $groupBy predicate.
     *
     * @param int|null $ttl acceptable time to live if you're willing to accept a cached version of this request
     *
     * @return array|int multidimensional array with int values if grouped by, otherwise int
     */
    public function getCount(array $query, ?string $groupBy = null, ?int $ttl = null);

    /**
     * Save the changes between $oldGraph -> $newGraph.
     *
     * @return bool true or throws exception on error
     */
    public function saveChanges(ExtendedGraph $oldGraph, ExtendedGraph $newGraph, ?string $context = null, ?string $description = null): bool;

    /**
     * Register an event hook, which will be executed when the event fires.
     */
    public function registerHook(string $eventType, IEventHook $hook): void;

    // START Deprecated methods that will be removed in 1.x.x

    /**
     * Return (DESCRIBE) according to a filter.
     *
     * @deprecated Use graph() instead
     *
     * @param array $filter conditions to filter by
     */
    public function describe(array $filter): ExtendedGraph;

    /**
     * Generates table rows.
     *
     * @deprecated calling save will generate table rows - this method seems to be only used in tests and does not belong on the interface
     */
    public function generateTableRows(string $tableType, ?string $resource = null, ?string $context = null): void;

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
     * @deprecated Search will be removed from a future version of Tripod as its functionality is equivalent to tables
     *
     * @return array results
     *
     * @throws Exception       - if search provider cannot be found
     * @throws SearchException - if something goes wrong
     */
    public function search(array $params): array;

    /**
     * Get any documents that were left in a locked state.
     *
     * @deprecated this is a feature of the mongo implementation - this method will move from the interface to the mongo-specific Driver class soon
     *
     * @param string|null $fromDateTime strtotime compatible string
     * @param string|null $tillDateTime strtotime compatible string
     *
     * @return array of locked documents
     */
    public function getLockedDocuments(?string $fromDateTime = null, ?string $tillDateTime = null): array;

    /**
     * Remove any inert locks left by a given transaction.
     *
     * @deprecated this is a feature of the mongo implementation - this method will move from the interface to the mongo-specific Driver class soon
     *
     * @return bool true or throws exception on error
     */
    public function removeInertLocks(string $transaction_id, string $reason): bool;

    // END Deprecated methods that will be removed in 1.x.x
}
