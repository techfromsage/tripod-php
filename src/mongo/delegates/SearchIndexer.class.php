<?php

namespace Tripod\Mongo\Composites;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR . 'mongo/delegates/SearchDocuments.class.php';
require_once TRIPOD_DIR . 'mongo/providers/MongoSearchProvider.class.php';
require_once TRIPOD_DIR . 'exceptions/SearchException.class.php';

use Tripod\Mongo\Config;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Labeller;
/**
 * Class SearchIndexer
 * @package Tripod\Mongo
 */
class SearchIndexer extends CompositeBase
{
    private $tripod = null;

    protected $labeller = null;

    protected $stat = null;

    /**
     * @var $configuredProvider \Tripod\ISearchProvider
     */
    private $configuredProvider = null;

    /**
     * @param \Tripod\Mongo\Driver $tripod
     * @throws \Tripod\Exceptions\SearchException
     */
    public function __construct(\Tripod\Mongo\Driver $tripod)
    {
        $this->tripod = $tripod;
        $this->storeName = $tripod->getStoreName();
        $this->podName = $tripod->podName;
        $this->labeller = new Labeller();
        $this->stat = $tripod->getStat();
        $this->config = Config::getInstance();
        $provider = $this->config->getSearchProviderClassName($this->tripod->getStoreName());

        if(class_exists($provider)){
            $this->configuredProvider = new $provider($this->tripod);
        } else {
            throw new \Tripod\Exceptions\SearchException("Did not recognise Search Provider, or could not find class: $provider");
        }
        $this->readPreference = \MongoClient::RP_PRIMARY;  // todo: figure out where this should go.
    }

    /**
     * Receive update from subject
     * @param ImpactedSubject
     * @return void
     */
    public function update(ImpactedSubject $subject)
    {
        $resource = $subject->getResourceId();
        $resourceUri    = $resource[_ID_RESOURCE];
        $context        = $resource[_ID_CONTEXT];

        $this->generateAndIndexSearchDocuments(
            $resourceUri,
            $context,
            $subject->getPodName(),
            $subject->getSpecTypes()
        );
    }

    /**
     * @return array
     */
    public function getTypesInSpecification()
    {
        return $this->config->getTypesInSearchSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * Returns the operation this composite can satisfy
     * @return string
     */
    public function getOperationType()
    {
        return OP_SEARCH;
    }

    /**
     * @param string $storeName
     * @param string $specId
     * @return array|null
     */
    public function getSpecification($storeName, $specId)
    {
        return $this->config->getSearchDocumentSpecification($storeName,$specId);
    }

    /**
     * Removes all existing documents for the supplied resource and regenerate the search documents
     * @param string $resourceUri
     * @param string $context
     * @param string $podName
     * @param array | string | null $specType
     */
    public function generateAndIndexSearchDocuments($resourceUri, $context, $podName, $specType = array())
    {
        $mongoCollection    = $this->config->getCollectionForCBD($this->storeName, $podName);

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

        foreach($documentsToIndex as $document) {
            if(!empty($document)) $searchProvider->indexDocument($document);
        }
    }

    /**
     * @param array $resourcesAndPredicates
     * @param string $context
     * @return array|mixed
     */
    public function findImpactedComposites(Array $resourcesAndPredicates, $context)
    {
        return $this->getSearchProvider()->findImpactedDocuments($resourcesAndPredicates, $context);
    }

    /**
     * @param string $typeId
     * @return array|bool
     */
    public function deleteSearchDocumentsByTypeId($typeId)
    {
    	return $this->getSearchProvider()->deleteSearchDocumentsByTypeId($typeId);
    }
    

    /**
     * @return \Tripod\ISearchProvider
     */
    protected function getSearchProvider()
    {
        return $this->configuredProvider;
    }

    /**
     * @param \MongoCollection $collection
     * @param string $context
     * @return \Tripod\Mongo\SearchDocuments
     */
    protected function getSearchDocumentGenerator(\MongoCollection $collection, $context )
    {
        return new \Tripod\Mongo\SearchDocuments($this->storeName, $collection, $context, $this->tripod->getStat());
    }

    /**
     * @param array $input
     * @return array
     */
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