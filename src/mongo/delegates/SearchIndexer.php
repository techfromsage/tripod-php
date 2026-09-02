<?php

declare(strict_types=1);

namespace Tripod\Mongo\Composites;

use MongoDB\Collection;
use MongoDB\Driver\ReadPreference;
use Tripod\Exceptions\SearchException;
use Tripod\ISearchProvider;
use Tripod\Mongo\Driver;
use Tripod\Mongo\IConfigInstance;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\ApplyOperation;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\SearchDocuments;
use Tripod\Timer;

class SearchIndexer extends CompositeBase
{
    private Driver $tripod;

    private ISearchProvider $searchProvider;

    /**
     * @throws SearchException
     */
    public function __construct(Driver $tripod, string $readPreference = ReadPreference::PRIMARY)
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
     * Receive update from subject.
     */
    public function update(ImpactedSubject $subject): void
    {
        $resource = $subject->getResourceId();
        $resourceUri = $resource[_ID_RESOURCE];
        $context = $resource[_ID_CONTEXT];

        $this->generateAndIndexSearchDocuments(
            $resourceUri,
            $context,
            $subject->getPodName(),
            $subject->getSpecTypes()
        );
    }

    public function getTypesInSpecifications(): array
    {
        return $this->config->getTypesInSearchSpecifications($this->storeName, $this->getPodName());
    }

    /**
     * Returns the operation this composite can satisfy.
     */
    public function getOperationType(): string
    {
        return OP_SEARCH;
    }

    public function getSpecification(string $storeName, string $specId): ?array
    {
        return $this->config->getSearchDocumentSpecification($storeName, $specId);
    }

    /**
     * Removes all existing documents for the supplied resource and regenerate the search documents.
     *
     * @param array|string|null $specType
     */
    public function generateAndIndexSearchDocuments(string $resourceUri, string $context, string $podName, $specType = []): void
    {
        $mongoCollection = $this->config->getCollectionForCBD($this->storeName, $podName);

        $searchDocGenerator = $this->getSearchDocumentGenerator($mongoCollection, $context);
        $searchProvider = $this->getSearchProvider();

        // 1. remove all search documents for this resource
        $searchProvider->deleteDocument($resourceUri, $context, $specType); // null means delete all documents for this resource

        // 2. regenerate search documents for this resource
        $documentsToIndex = [];
        // first work out what its type is
        $query = [_ID_KEY => [
            _ID_RESOURCE => $this->labeller->uri_to_alias($resourceUri),
            _ID_CONTEXT => $this->getContextAlias($context),
        ]];

        $resourceAndType = $mongoCollection->find(
            $query,
            [
                'projection' => [_ID_KEY => 1, 'rdf:type' => 1],
                'maxTimeMS' => $this->config->getMongoCursorTimeout(),
            ]
        );
        foreach ($resourceAndType as $rt) {
            if (isset($rt['rdf:type'])) {
                $rdfTypes = [];

                if (isset($rt['rdf:type'][VALUE_URI])) {
                    $rdfTypes[] = $rt['rdf:type'][VALUE_URI];
                } else {
                    // an array of types
                    foreach ($rt['rdf:type'] as $type) {
                        if (isset($type[VALUE_URI])) {
                            $rdfTypes[] = $type[VALUE_URI];
                        }
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
     * @return array|null Will return an array with a count and group id, if $queueName is sent and $resourceUri is null
     */
    public function generateSearchDocuments(
        string $searchDocumentType,
        ?string $resourceUri = null,
        ?string $context = null,
        ?string $queueName = null
    ): ?array {
        $t = new Timer();
        $t->start();
        // default the context
        $contextAlias = $this->getContextAlias($context);
        $spec = $this->getConfigInstance()->getSearchDocumentSpecification($this->getStoreName(), $searchDocumentType);
        if ($spec === null) {
            throw new SearchException('Could not find a search document specification for ' . $searchDocumentType);
        }

        if ($resourceUri) {
            $this->generateAndIndexSearchDocuments($resourceUri, $contextAlias, $spec['from'], $searchDocumentType);

            return null;
        }

        // default collection
        $from = $spec['from'] ?? $this->podName;

        $types = [];
        if (isset($spec[_ID_TYPE])) {
            if (is_array($spec[_ID_TYPE])) {
                foreach ($spec[_ID_TYPE] as $type) {
                    if (!is_string($type)) {
                        continue;
                    }
                    $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($type)];
                    $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($type)];
                }
            } elseif (is_string($spec[_ID_TYPE])) {
                $types[] = ['rdf:type.u' => $this->labeller->qname_to_alias($spec[_ID_TYPE])];
                $types[] = ['rdf:type.u' => $this->labeller->uri_to_alias($spec[_ID_TYPE])];
            }
        }

        $filter = ['$or' => $types];

        $count = $this->getConfigInstance()->getCollectionForCBD($this->getStoreName(), $from)->count($filter);
        $docs = $this->getConfigInstance()
            ->getCollectionForCBD($this->getStoreName(), $from)
            ->find(
                $filter,
                ['maxTimeMS' => $this->getConfigInstance()->getMongoCursorTimeout()]
            );

        $jobOptions = [];
        $subjects = [];
        if ($queueName) {
            $jobOptions['statsConfig'] = $this->getStatsConfig();
            $jobGroup = $this->getJobGroup($this->storeName);
            $jobOptions[ApplyOperation::TRACKING_KEY] = $jobGroup->getId()->__toString();
            $jobGroup->setJobCount($count);
        }

        foreach ($docs as $doc) {
            if ($queueName) {
                $subject = new ImpactedSubject(
                    $doc[_ID_KEY],
                    OP_SEARCH,
                    $this->storeName,
                    $from,
                    [$searchDocumentType]
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

        if ($subjects !== []) {
            $this->queueApplyJob($subjects, $queueName, $jobOptions);
        }

        $t->stop();
        $this->timingLog(MONGO_CREATE_TABLE, [
            'type' => $spec[_ID_TYPE] ?? null,
            'duration' => $t->result(),
            'filter' => $filter,
            'from' => $from,
        ]);
        $this->getStat()->timer(MONGO_CREATE_SEARCH_DOC . ('.' . $searchDocumentType), $t->result());

        $stat = ['count' => $count];
        if (isset($jobOptions[ApplyOperation::TRACKING_KEY])) {
            $stat[ApplyOperation::TRACKING_KEY] = $jobOptions[ApplyOperation::TRACKING_KEY];
        }

        return $stat;
    }

    public function findImpactedComposites(array $resourcesAndPredicates, string $contextAlias): array
    {
        return $this->getSearchProvider()->findImpactedDocuments($resourcesAndPredicates, $contextAlias);
    }

    public function deleteSearchDocumentsByTypeId(string $typeId): int
    {
        return $this->getSearchProvider()->deleteSearchDocumentsByTypeId($typeId);
    }

    protected function getSearchProvider(): ISearchProvider
    {
        return $this->searchProvider;
    }

    protected function getSearchDocumentGenerator(Collection $collection, string $context): SearchDocuments
    {
        return new SearchDocuments($this->storeName, $collection, $context, $this->tripod->getStat());
    }

    protected function deDupe(array $input): array
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
     * For mocking.
     *
     * @param Driver          $tripod Mongo Tripod Driver
     * @param IConfigInstance $config Mongo Tripod ConfigInstance
     *
     * @throws SearchException If provider class cannot be found
     */
    protected function setSearchProvider(Driver $tripod, ?IConfigInstance $config = null): void
    {
        if (is_null($config)) {
            $config = $this->getConfigInstance();
        }

        $provider = $config->getSearchProviderClassName($tripod->getStoreName());
        if ($provider !== null && is_a($provider, ISearchProvider::class, true)) {
            $this->searchProvider = new $provider($tripod);
        } else {
            throw new SearchException(
                'Did not recognise Search Provider, or could not find class: ' . $provider
            );
        }
    }
}
