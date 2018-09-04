<?php

namespace Tripod\Mongo\Composites;

require_once TRIPOD_DIR . 'mongo/MongoTripodConstants.php';
require_once TRIPOD_DIR . 'mongo/delegates/SearchDocuments.class.php';
require_once TRIPOD_DIR . 'mongo/providers/MongoSearchProvider.class.php';
require_once TRIPOD_DIR . 'exceptions/SearchException.class.php';

use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\Jobs\ApplyOperation;
use \MongoDB\Driver\ReadPreference;
use \MongoDB\Collection;
use Tripod\Mongo\Config;
use Tripod\Mongo\Driver;

/**
 * Class SearchIndexer
 * @package Tripod\Mongo\Composites
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
     * @param string $readPreference
     * @throws \Tripod\Exceptions\SearchException
     */
    public function __construct(\Tripod\Mongo\Driver $tripod, $readPreference = ReadPreference::RP_PRIMARY)
    {
        $this->tripod = $tripod;
        $this->storeName = $tripod->getStoreName();
        $this->podName = $tripod->podName;
        $this->labeller = new Labeller();
        $this->stat = $tripod->getStat();
        $this->config = $this->getConfigInstance();
        $this->setSearchProvider($this->tripod, $this->config);
        $this->readPreference = $readPreference;
    }

    /**
     * Receive update from subject
     * @param ImpactedSubject $subject
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
    public function getTypesInSpecifications()
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
        return $this->config->getSearchDocumentSpecification($storeName, $specId);
    }

    /**
     * Removes all existing documents for the supplied resource and regenerate the search documents
     * @param string $resourceUri
     * @param string $context
     * @param string $podName
     * @param array | string | null $specType
     */
    public function generateAndIndexSearchDocuments($resourceUri, $context, $podName, $specType = [])
    {
        $mongoCollection    = $this->config->getCollectionForCBD($this->storeName, $podName);

        $searchDocGenerator = $this->getSearchDocumentGenerator($mongoCollection, $context);
        $searchProvider = $this->getSearchProvider();

        //1. remove all search documents for this resource
        $searchProvider->deleteDocument($resourceUri, $context, $specType); // null means delete all documents for this resource

        //2. regenerate search documents for this resource
        $documentsToIndex   = [];
        // first work out what its type is
        $query = array("_id"=>array(
            'r'=>$this->labeller->uri_to_alias($resourceUri),
            'c'=>$this->getContextAlias($context)
        ));

        $resourceAndType = $mongoCollection->find(
            $query,
            [
                'projection' => ["_id" => 1, "rdf:type" => 1],
                'maxTimeMS' => $this->config->getMongoCursorTimeout()
            ]
        );
        foreach ($resourceAndType as $rt) {
            if (array_key_exists("rdf:type", $rt)) {
                $rdfTypes = [];

                if (array_key_exists('u', $rt["rdf:type"])) {
                    $rdfTypes[] = $rt["rdf:type"]['u'];
                } else {
                    // an array of types
                    foreach ($rt["rdf:type"] as $type) {
                        $rdfTypes[] = $type['u'];
                    }
                }

                $docs = $searchDocGenerator->generateSearchDocumentsBasedOnRdfTypes($rdfTypes, $resourceUri, $context);
                foreach ($docs as $d) {
                    $documentsToIndex[] = $d;
                }
            }
        }

        foreach ($documentsToIndex as $document) {
            if (!empty($document)) {
                $searchProvider->indexDocument($document);
            }
        }
    }

    /**
     * @param string $searchDocumentType
     * @param string|null $resourceUri
     * @param string|null $context
     * @param string|null $queueName
     * @return array|null Will return an array with a count and group id, if $queueName is sent and $resourceUri is null
     */
    public function generateSearchDocuments(
        $searchDocumentType,
        $resourceUri = null,
        $context = null,
        $queueName = null
    ) {
        $t = new \Tripod\Timer();
        $t->start();
        // default the context
        $contextAlias = $this->getContextAlias($context);
        $spec = $this->getConfigInstance()->getSearchDocumentSpecification($this->getStoreName(), $searchDocumentType);

        if ($resourceUri) {
            $this->generateAndIndexSearchDocuments($resourceUri, $contextAlias, $spec['from'], $searchDocumentType);
            return;
        }


        // default collection
        $from = (isset($spec["from"])) ? $spec["from"] : $this->podName;

        $types = [];
        if (is_array($spec["type"])) {
            foreach ($spec["type"] as $type) {
                $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($type));
                $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($type));
            }
        } else {
            $types[] = array("rdf:type.u"=>$this->labeller->qname_to_alias($spec["type"]));
            $types[] = array("rdf:type.u"=>$this->labeller->uri_to_alias($spec["type"]));
        }
        $filter = array('$or'=> $types);
        if (isset($resource)) {
            $filter["_id"] = array(_ID_RESOURCE=>$this->labeller->uri_to_alias($resource),_ID_CONTEXT=>$contextAlias);
        }

        $count = $this->getConfigInstance()->getCollectionForCBD($this->getStoreName(), $from)->count($filter);
        $docs = $this->getConfigInstance()
            ->getCollectionForCBD($this->getStoreName(), $from)
            ->find(
                $filter,
                ['maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout()]
            );

        $jobOptions = [];
        $subjects = [];
        if ($queueName && !$resourceUri) {
            $jobOptions['statsConfig'] = $this->getStatsConfig();
            $jobGroup = $this->getJobGroup($this->storeName);
            $jobOptions[ApplyOperation::TRACKING_KEY] = $jobGroup->getId()->__toString();
            $jobGroup->setJobCount($count);
        }
        foreach ($docs as $doc) {
            if ($queueName && !$resourceUri) {
                $subject = new ImpactedSubject(
                    $doc['_id'],
                    OP_SEARCH,
                    $this->storeName,
                    $from,
                    array($searchDocumentType)
                );

                $subjects[] = $subject;
                // Queue ApplyOperations jobs in batches rather than individually
                if (count($subjects) >= $this->getConfigInstance()->getBatchSize(OP_SEARCH)) {
                    $this->queueApplyJob($subjects, $queueName, $jobOptions);
                    $subjects = [];
                }
            } else {
                $this->generateAndIndexSearchDocuments(
                    $doc[_ID_KEY][_ID_RESOURCE],
                    $doc[_ID_KEY][_ID_CONTEXT],
                    $spec['from'],
                    $searchDocumentType
                );
            }
        }

        if (!empty($subjects)) {
            $this->queueApplyJob($subjects, $queueName, $jobOptions);
        }

        $t->stop();
        $this->timingLog(MONGO_CREATE_TABLE, array(
            'type'=>$spec['type'],
            'duration'=>$t->result(),
            'filter'=>$filter,
            'from'=>$from));
        $this->getStat()->timer(MONGO_CREATE_SEARCH_DOC.".$searchDocumentType", $t->result());

        $stat = ['count' => $count];
        if (isset($jobOptions[ApplyOperation::TRACKING_KEY])) {
            $stat[ApplyOperation::TRACKING_KEY] = $jobOptions[ApplyOperation::TRACKING_KEY];
        }
        return $stat;
    }

    /**
     * @param array $resourcesAndPredicates
     * @param string $context
     * @return array|mixed
     */
    public function findImpactedComposites(array $resourcesAndPredicates, $context)
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
     * @param Collection $collection
     * @param string $context
     * @return \Tripod\Mongo\SearchDocuments
     */
    protected function getSearchDocumentGenerator(Collection $collection, $context)
    {
        return new \Tripod\Mongo\SearchDocuments($this->storeName, $collection, $context, $this->tripod->getStat());
    }

    /**
     * @param array $input
     * @return array
     */
    protected function deDupe(array $input)
    {
        $output = [];
        foreach ($input as $i) {
            if (!in_array($i, $output)) {
                $output[] = $i;
            }
        }
        return $output;
    }

    /**
     * For mocking
     *
     * @param Driver                        $tripod Mongo Tripod Driver
     * @param \Tripod\Mongo\IConfigInstance $config Mongo Tripod ConfigInstance
     * @return void
     * @throws \Tripod\Exceptions\SearchException If provider class cannot be found
     */
    protected function setSearchProvider(Driver $tripod, \Tripod\Mongo\IConfigInstance $config = null)
    {
        if (is_null($config)) {
            $config = $this->getConfigInstance();
        }

        $provider = $config->getSearchProviderClassName($tripod->getStoreName());
        if (class_exists($provider)) {
            $this->configuredProvider = new $provider($tripod);
        } else {
            throw new \Tripod\Exceptions\SearchException(
                "Did not recognise Search Provider, or could not find class: $provider"
            );
        }
    }
}
