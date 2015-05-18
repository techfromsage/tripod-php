<?php

namespace Tripod;

/**
 * Class ITripodSearchProvider
 * @package Tripod
 */
interface ITripodSearchProvider
{
    /**
     * Indexes the given document
     * @param array $document the document to index
     * @throws \Tripod\Exceptions\SearchException if there was an error indexing the document
     * @return mixed
     */
    public function indexDocument($document);

    /**
     * Removes a single document from the search index based on the specified resource and context and spec id.
     * If spec id is not specified this method will delete all search documents that match the resource and context.
     * @param string $resource
     * @param string $context
     * @param array | string | null $specId
     * @throws \Tripod\Exceptions\SearchException if there was an error removing the document
     * @return mixed
     */
    public function deleteDocument($resource, $context, $specId=array());

    /**
     * Returns the ids of all documents that contain and impact index entry
     * matching the resource and context specified
     * @param array $resourcesAndPredicates
     * @param string $context
     * @internal param $resource
     * @return array the ids of search documents that had matching entries in their impact index
     */
    public function findImpactedDocuments(array $resourcesAndPredicates, $context);

    /**
     * Executes the query and returns a structure representing a search results.
     * the structure has the following two properties:
     *  head: which contains :
     *      total       - the total number of results found
     *      limit       - the maximum number of results returning per page
     *      offset      - the offset we skipped to
     *      query       - the user entered query
     *      query_terms - the terms that the query was split into
     *      duration    - the time it took to execute the query and build the result structure
     *  results     - an array of results
     *
     * @param string $q the query as input by a user
     * @param string $type the search document type to restrict results in other words _id.type
     * @param array $indices an array of indices (from spec) to match query terms against, must specify at least one
     * @param array $fields  an array of the fields (from spec) you want included in the search results
     * @param int $limit  the number of results to return per page
     * @param int $offset  the offset to skip to
     * @return mixed  a structure representing the search results
     * @throws \Tripod\Exceptions\SearchException if there was an error performing the search, or if the parameters are invalid
     */
    public function search($q, $type, $indices=array(), $fields=array(), $limit=10, $offset=0);
    
    /**
     * Removes all documents from search index based on the specified type id.
     * Here search type id represents to id from, mongo tripod config, that is converted to _id.type in SEARCH_INDEX_COLLECTION
     * If type id is not specified this method will throw an exception.      
     * @param string $typeId search type id
     * @return bool|array  response returned by mongo
     * @throws \Tripod\Exceptions\Exception if there was an error performing the operation
     */
    public function deleteSearchDocumentsByTypeId($typeId);
}