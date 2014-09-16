<?php
require_once TRIPOD_DIR.'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR.'mongo/delegates/MongoTripodSearchDocuments.class.php';
require_once TRIPOD_DIR.'mongo/providers/ITripodSearchProvider.php';
require_once TRIPOD_DIR.'classes/Timer.class.php';

class MongoSearchProvider implements ITripodSearchProvider
{
    private $tripod = null;

    private $labeller = null;

    private $stopWords = array("a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours ", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves");

    public function __construct(MongoTripod $tripod)
    {
        $this->tripod = $tripod;
        $this->labeller = new MongoTripodLabeller();
    }

    /**
     * Indexes the given document
     * @param array $document the document to index
     * @throws TripodSearchException if there was an error indexing the document
     * @return mixed
     */
    public function indexDocument($document)
    {
        try {
            $this->tripod->db->selectCollection($this->getSearchCollectionName())->ensureIndex(array('_id.type' => 1), array('background' => 1));
            $this->tripod->db->selectCollection($this->getSearchCollectionName())->ensureIndex(array('_id.r' => 1, '_id.c' => 1), array('background' => 1));
            $this->tripod->db->selectCollection($this->getSearchCollectionName())->ensureIndex(array('_impactIndex' => 1), array('background' => 1));
            $this->tripod->db->selectCollection($this->getSearchCollectionName())->save($document);
        } catch (Exception $e) {
            throw new TripodSearchException("Failed to Index Document \n" . print_r($document, true), 0, $e);
        }
    }

    /**
     * Removes a single document from the search index based on the specified resource and context and spec id.
     * If spec id is not specified this method will delete all search documents that match the resource and context.
     * @param string $resource
     * @param string $context
     * @param string|null $specId
     * @throws TripodSearchException if there was an error removing the document
     * @return mixed
     */
    public function deleteDocument($resource, $context, $specId = null)
    {
        try {
            $query = null;
            if (empty($specId)) {
                $query = array('_id.r' => $this->labeller->uri_to_alias($resource), '_id.c' => $context);
            } else {
                $query = array('_id' => array('r' => $this->labeller->uri_to_alias($resource), 'c' => $context, 'type' => $specId));
            }
            $this->tripod->db->selectCollection($this->getSearchCollectionName())->remove($query);
        } catch (Exception $e) {
            throw new TripodSearchException("Failed to Remove Document with id \n" . print_r($query, true), 0, $e);
        }
    }

    /**
     * Returns the ids of all documents that contain and impact index entry
     * matching the resource and context specified
     * @param array $resourcesAndPredicates
     * @param string $context
     * @return array the ids of search documents that had matching entries in their impact index
     */
    public function findImpactedDocuments(array $resourcesAndPredicates, $context)
    {
        $contextAlias = $this->labeller->uri_to_alias($context);

        $specPredicates = array();

        foreach(MongoTripodConfig::getInstance()->getSearchDocumentSpecifications() as $spec)
        {
            if(isset($spec[_ID_KEY]))
            {
                $spec[$spec[_ID_KEY]] = MongoTripodConfig::getInstance()->getDefinedPredicatesInSpec($spec[_ID_KEY]);
            }
        }

        // build a filter - will be used for impactIndex detection and finding search types to re-gen
        $searchDocFilters = array();
        $resourceFilters = array();
        foreach ($resourcesAndPredicates as $resource=>$resourcePredicates)
        {
            $resourceAlias = $this->labeller->uri_to_alias($resource);
            $id = array(_ID_RESOURCE=>$resourceAlias,_ID_CONTEXT=>$contextAlias);
            // If we don't have a working config or there are no predicates listed, remove all
            // rows associated with the resource in all search types
            if(empty($specPredicates) || empty($resourcePredicates))
            {
                // build $filter for queries to impact index
                $resourceFilters[] = $id;
            }
            else
            {
                foreach($specPredicates as $searchDocType=>$predicates)
                {
                    // Only look for search rows if the changed predicates are actually defined in the searchDocspec
                    if(array_intersect($resourcePredicates, $predicates))
                    {
                        if(!isset($searchDocFilters[$searchDocType]))
                        {
                            $searchDocFilters[$searchDocType] = array();
                        }
                        // build $filter for queries to impact index
                        $searchDocFilters[$searchDocType][] = $id;
                    }
                }
            }

        }

        if(empty($searchDocFilters) && !empty($resourceFilters))
        {
            $query = array("value."._IMPACT_INDEX=>array('$in'=>$resourceFilters));
        }
        else
        {
            $query = array();
            foreach($searchDocFilters as $searchDocType=>$filters)
            {
                // first re-gen views where resources appear in the impact index
                $query[] = array("value."._IMPACT_INDEX=>array('$in'=>$filters), '_id'._ID_TYPE=>$searchDocType);
            }

            if(!empty($resourceFilters))
            {
                $query[] = array("value."._IMPACT_INDEX=>array('$in'=>$resourceFilters));
            }

            if(count(array_keys($query)) > 1)
            {
                $query = array('$or'=>$query);
            }
        }
        if(empty($query))
        {
            return array();
        }
        $cursor = $this->tripod->db->selectCollection($this->getSearchCollectionName())->find($query, array('_id'=>true));

        return iterator_to_array($cursor);
    }

    public function search($q, $type, $indices=array(), $fields=array(), $limit=10, $offset=0)
    {
        if(empty($q))       { throw new TripodSearchException("You must specify a query"); }
        if(empty($type))    { throw new TripodSearchException("You must specify the search document type to restrict the query to"); }
        if(empty($indices)) { throw new TripodSearchException("You must specify at least one index from the search document specification to query against"); }
        if(empty($fields))  { throw new TripodSearchException("You must specify at least one field from the search document specification to return"); }

        if(!is_numeric($limit) || $limit < 0 ){
            throw new TripodSearchException("Value for limit must be a positive number");
        }

        if(!is_numeric($offset) || $offset < 0 ){
            throw new TripodSearchException("Value for offset must be a positive number");
        }

        $original_terms = explode(" ",trim(strtolower($q)));
        $terms = array_values(array_diff($original_terms, $this->stopWords));

        // todo: this means if all the words entered were stop words, then use the orginal terms rather than do nothing!
        if(empty($terms)) $terms = $original_terms;

        $regexes = array();
        foreach($terms as $t){
            $regexes[] = new MongoRegex("/{$t}/");
        }

        $query = array();
        $query['_id.type'] = $type;

        if(count($indices) == 1) {
            $searchIndex = $indices[0];
            $query[$searchIndex] = array('$all'=>$regexes);
        } else {
            $query['$or'] = array();
            foreach($indices as $searchIndex){
                $query['$or'][] = array("$searchIndex"=>array('$all'=>$regexes));
            }
        }

        $fieldsToReturn = array();
        foreach($fields as $field){
            $fieldsToReturn[$field] = 1;
        }
        $searchTimer = new Timer();
        $searchTimer->start();
        $cursor = $this->tripod->db->selectCollection($this->getSearchCollectionName())->find($query, $fieldsToReturn)->limit($limit)->skip($offset);
        $searchResults = array();
        $searchResults['head'] = array();
        $searchResults['head']['count']     = "";
        $searchResults['head']['limit']     = $limit;
        $searchResults['head']['offset']    = $offset;
        $searchResults['head']['duration']  = "";
        $searchResults['head']['query']     = $q;
        $searchResults['head']['query_terms_used'] = $terms;
        $searchResults['results']   = array();

        if($cursor->count() > 0) {
            $searchResults['head']['count']     = $cursor->count();

            while($cursor->hasNext()){
                $result = $cursor->getNext();

                // if more than one field has been asked for we need to
                // enumerate them in the results returned. However if only one has been
                // asked for then results is just set to that single fields value.
                if(count($fields) > 1){
                    $r = array();
                    foreach($fields as $field){
                        if(isset($result[$field])){
                            $r[$field] = $result[$field];
                        } else {
                            $r[$field] = "";
                        }
                    }
                    $searchResults['results'][] = $r;
                } else {
                    $searchResults['results'][] = $result[$fields[0]];
                }

            }
        } else {
            $searchResults['head']['count'] = 0;
        }
        $searchTimer->stop();
        $searchResults['head']["duration"] = $searchTimer->result() . ' ms';
        return $searchResults;
    }

    public function getSearchCollectionName()
    {
        return SEARCH_INDEX_COLLECTION;
    }
    
    /**
     * Removes all documents from search index based on the specified type id.
     * Here search type id represents to id from, mongo tripod config, that is converted to _id.type in SEARCH_INDEX_COLLECTION
     * If type id is not specified this method will throw an exception.
     * @param string $typeId search type id
     * @return bool|array  response returned by mongo
     * @throws TripodException if there was an error performing the operation
     */
    public function deleteSearchDocumentsByTypeId($typeId)
    {
    	$searchSpec = $this->getSearchDocumentSpecification($typeId);
    	if ($searchSpec == null)
    	{    		
    		throw new TripodSearchException("Cound not find a search specification for $typeId");
    	}
    	    	
    	return $this->tripod->db->selectCollection($this->getSearchCollectionName())->remove(array("_id.type" => $typeId));
    }
    
    protected function getSearchDocumentSpecification($typeId)
    {
    	return MongoTripodConfig::getInstance()->getSearchDocumentSpecification($typeId);
    }
}