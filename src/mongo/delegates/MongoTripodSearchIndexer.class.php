<?php
    require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';
    require_once TRIPOD_DIR . 'mongo/delegates/MongoTripodSearchDocuments.class.php';
    require_once TRIPOD_DIR . 'mongo/providers/MongoSearchProvider.class.php';
    require_once TRIPOD_DIR . 'exceptions/TripodSearchException.class.php';


class MongoTripodSearchIndexer extends MongoTripodBase implements SplObserver
{
    private $tripod = null;

    protected $labeller = null;

    protected $stat = null;

    /**
     * @var $configuredProvider ITripodSearchProvider
     */
    private $configuredProvider = null;

    public function __construct(MongoTripod $tripod)
    {
        $this->tripod = $tripod;
        $this->labeller = new MongoTripodLabeller();
        $this->stat = $tripod->getStat();

        $provider = MongoTripodConfig::getInstance()->getSearchProviderClassName();

        if(class_exists($provider)){
            $this->configuredProvider = new $provider($this->tripod);
        } else {
            throw new TripodSearchException("Did not recognise Search Provider, or could not find class: $provider");
        }
    }

    /**
     * (PHP 5 &gt;= 5.1.0)<br/>
     * Receive update from subject
     * @link http://php.net/manual/en/splobserver.update.php
     * @param SplSubject $subject <p>
     * The <b>SplSubject</b> notifying the observer of an update.
     * </p>
     * @return void
     */
    public function update(SplSubject $subject)
    {
        /* @var $subject ModifiedSubject */
        $queuedItem = $subject->getData();

        $resourceUri    = $queuedItem['r'];
        $context        = $queuedItem['c'];

        $collectionName = $queuedItem['collection'];

        $specTypes = null;

        if(isset($queuedItem['specTypes']))
        {
            $specTypes = $queuedItem['specTypes'];
        }

        $this->generateAndIndexSearchDocuments($resourceUri, $context, $collectionName, $specTypes);
    }

    /**
     * Removes all existing documents for the supplied resource and regenerate the search documents
     * @param string $resourceUri
     * @param string $context
     * @param string $collectionName
     * @param array | string | null $specType
     */
    public function generateAndIndexSearchDocuments($resourceUri, $context, $collectionName, $specType = null)
    {
        $mongoCollection = $this->config->getCollectionForCBD($collectionName);

        $searchDocGenerator = $this->getSearchDocumentGenerator($mongoCollection, $context);
        $searchProvider = $this->getSearchProvider();

        //1. remove all search documents for this resource
        $searchProvider->deleteDocument($resourceUri, $context, $specType); // null means delete all documents for this resource

        //2. find all impacted documents and regenerate them
        $documentsToIndex   = array();

        //3. regenerate search documents for this resource
        // first work out what its type is
        $query = array("_id"=>array(
            'r'=>$this->labeller->uri_to_alias($resourceUri),
            'c'=>$this->getContextAlias($context)
        ));

        $resourceAndType = $mongoCollection->find($query,array("_id"=>1,"rdf:type"=>1));

        foreach ($resourceAndType as $rt)
        {
            if (array_key_exists("rdf:type",$rt))
            {

                $rdfTypes = array();

                if (array_key_exists('u',$rt["rdf:type"]))
                {
                    $rdfTypes[] = $rt["rdf:type"]['u'];
                }
                else
                {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type)
                    {
                        $rdfTypes[] = $type['u'];
                    }
                }

                $docs = $searchDocGenerator->generateSearchDocumentsBasedOnRdfTypes($rdfTypes, $resourceUri, $context);
                foreach($docs as $d){
                    $documentsToIndex[] = $d;
                }

            }
        }

//        $documentsToDelete = $this->deDupe($documentsToDelete);
//
//        foreach($documentsToDelete as $document){
//            if(!empty($document)) $searchProvider->deleteDocument($document['r'],$document['c'], $document['type']);
//        }

        foreach($documentsToIndex as $document) {
            if(!empty($document)) $searchProvider->indexDocument($document);
        }
    }

    public function findImpactedSearchDocuments($resourcesAndPredicates, $context)
    {
        return $this->getSearchProvider()->findImpactedDocuments($resourcesAndPredicates, $context);
    }

    public function deleteSearchDocumentsByTypeId($typeId)
    {
    	return $this->getSearchProvider()->deleteSearchDocumentsByTypeId($typeId);
    }
    

    /**
     * @return ITripodSearchProvider
     */
    protected function getSearchProvider()
    {
        return $this->configuredProvider;
    }

    /**
     * @param MongoCollection $collection
     * @param string $context
     * @return MongoTripodSearchDocuments
     */
    protected function getSearchDocumentGenerator($collection, $context )
    {
        return new MongoTripodSearchDocuments($collection, $context, $this->tripod->getStat());
    }

    protected function deDupe(Array $input)
    {
        $output = array();
        foreach($input as $i){
            if(!in_array($i, $output)){
                $output[] = $i;
            }
        }
        return $output;
    }

}