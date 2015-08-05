<?php

namespace Tripod;

/**
 * Class IDriver
 * @package Tripod
 */
interface IDriver
{
    public function graph($query,$includeProperties=array());

    /**
     * @deprecated
     * @abstract
     * @param $query
     * @return mixed
     */
    public function describe($query);

    /**
     * Return (DESCRIBE) the concise bound description of a resource
     * @param $resource string uri resource you'd like to describe
     * @param null $context string uri of the context, or named graph, you'd like to describe from
     * @return ExtendedGraph
     */
    public function describeResource($resource,$context=null);

    /**
     * Return (DESCRIBE) the concise bound descriptions of a bunch of resources
     * @param array $resources uris of resources you'd like to describe
     * @param null $context string uri of the context, or named graph, you'd like to describe from
     * @return ExtendedGraph
     */
    public function describeResources(Array $resources,$context=null);

    /**
     * Get a view of a given type for a given resource
     * @param $resource string uri of the resource you'd like the view for
     * @param $viewType string type of view
     * @return ExtendedGraph
     */
    public function getViewForResource($resource,$viewType);

    /**
     * Get views for multiple resources in one graph
     * @param array $resources uris of resources you'd like to describe
     * @param $viewType string type of view
     * @return ExtendedGraph
     */
    public function getViewForResources(Array $resources,$viewType);

    /**
     * Get views based on a pattern-match $filter
     * @param array $filter pattern to match to select views
     * @param $viewType string type of view
     * @return ExtendedGraph
     */
    public function getViews(Array $filter,$viewType);

    /**
     * Returns the etag of a resource, useful for caching
     */
    public function getETag($resource,$context=null);

    /**
     * Select data in a tabular format
     * @param array $filter pattern to match to select views
     * @param $fields
     * @param null $sortBy
     * @param null $limit
     * @param null $context
     * @return array
     */
    public function select($filter,$fields,$sortBy=null,$limit=null,$context=null);

    /**
     * Select data from a table
     * @param $tableType
     * @param array $filter
     * @param array $sortBy
     * @param int $offset
     * @param int $limit
     * @return mixed
     */
    public function getTableRows($tableType,$filter=array(),$sortBy=array(),$offset=0,$limit=10);

    /**
     * todo: work out what this does
     * @param $tableType
     * @param $fieldName
     * @param array $filter
     * @return mixed
     */
    public function getDistinctTableColumnValues($tableType, $fieldName, array $filter = array());


    /**
     * Get a count of resources matching the pattern in $query. Optionally group counts by specifying a $groupBy predicate
     * @param $query
     * @param null $groupBy
     * @return mixed
     */
    public function getCount($query,$groupBy=null);

    /**
     * Save the changes between $oldGraph -> $newGraph
     * @param ExtendedGraph $oldGraph
     * @param ExtendedGraph $newGraph
     * @param null $context
     * @param null $description
     * @return mixed
     */
    public function saveChanges(ExtendedGraph $oldGraph, ExtendedGraph $newGraph,$context=null,$description=null);

    /**
     * Register an event hook, which
     * @param $eventType
     * @param IEventHook $
     * @return mixed
     */
    public function registerHook($eventType,IEventHook $hook);

    /**
     * Generates table rows
     * @deprecated calling save will generate table rows - this method seems to be only used in tests and does not belong on the interface
     * @param $tableType
     * @param null $resource
     * @param null $context
     * @return mixed
     */
    public function generateTableRows($tableType,$resource=null,$context=null);

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
     * throw SearchException if any of the params are invalid
     *
     * @deprecated Search will be removed from a future version of Tripod as its functionality is equivalent to tables
     * @param Array $params
     * @throws \Tripod\Exceptions\Exception - if search provider cannot be found
     * @throws \Tripod\Exceptions\SearchException - if something goes wrong
     * @return Array results
     */
    public function search(Array $params);

    /**
     * Get any documents that were left in a locked state
     * @deprecated this is a feature of the mongo implementation - this method will move from the interface to the mongo-specific Driver class soon.
     * @param null $fromDateTime
     * @param null $tillDateTime
     * @return mixed
     */
    public function getLockedDocuments($fromDateTime =null , $tillDateTime = null);

    /**
     * Remove any inert locks left by a given transaction
     * @deprecated this is a feature of the mongo implementation - this method will move from the interface to the mongo-specific Driver class soon.
     * @param $transaction_id
     * @param $reason
     * @return mixed
     */
    public function removeInertLocks($transaction_id, $reason);

}