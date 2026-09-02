<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\ExtendedGraph;
use Tripod\Mongo\Composites\SearchIndexer;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\SearchDocuments;
use Tripod\Mongo\Updates;

class MongoTripodSearchDocumentsTest extends MongoTripodTestBase
{
    private string $defaultContext = 'http://talisaspire.com/';

    private string $defaultStoreName = 'tripod_php_testing';

    private string $defaultPodName = 'CBD_testing';

    protected function setUp(): void
    {
        parent::setUp();

        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->getTripodCollection($this->tripod)->drop();
        $this->loadBaseSearchDataViaTripod();
        foreach (Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }
    }

    public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyResource(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Resource must be specified');
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', null, 'http://talisaspire.com/');
    }

    public function testGenerateSearchDocumentBasedOnSpecIdThrowsExceptionWithEmptyContext(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Context must be specified');
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', null);
    }

    public function testGenerateSearchDocumentBasedOnSpecIdReturnNullForInvalidSearchSpecId(): void
    {
        $mockSearchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();

        $mockSearchDocuments->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->willReturn(null);
        $generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_something', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
        $this->assertNull($generatedDocuments);
    }

    public function testGenerateSearchDocumentBasedOnSpecIdReturnNullIfNoMatchForResourceFound(): void
    {
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resource/1', 'http://talisaspire.com/');
        $this->assertNull($generatedDocuments);
    }

    public function testGenerateSearchDocumentBasedOnSpecId(): void
    {
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');
        $this->assertNotNull($generatedDocuments);
        $this->assertEquals('http://talisaspire.com/resources/doc1', $generatedDocuments['_id']['r']);
    }

    public function testGenerateSearchDocumentPreservesDiacritics(): void
    {
        $searchDocuments = $this->getSearchDocuments($this->tripod);
        $generatedDocuments = $searchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc13', 'http://talisaspire.com/');
        $this->assertNotNull($generatedDocuments);
        $this->assertEquals('René Chapus', $generatedDocuments['result']['author']);
        $this->assertContains('rené chapus', $generatedDocuments['search_terms']);
        $this->assertEquals('http://talisaspire.com/resources/doc13', $generatedDocuments['_id']['r']);
    }

    public function testGenerateSearchDocumentHandlesNonStringLiterals(): void
    {
        $doc = [
            '_id' => [
                'r' => 'http://talisaspire.com/resources/typedDoc1',
                'c' => 'http://talisaspire.com/',
            ],
            '_version' => 0,
            'dct:title' => [
                'l' => 'Typed PHP: Stronger Types For Cleaner Code',
            ],
            'rdf:type' => [
                ['u' => 'http://purl.org/ontology/bibo/Book'],
            ],
            'dct:date' => ['l' => 2016],
            'temp:price' => ['l' => 19.99],
            'temp:isHardcover' => ['l' => true],
        ];

        $searchSpecs = [
            '_id' => 'i_search_resource',
            'type' => ['bibo:Book'],
            'from' => 'CBD_testing',
            'filter' => [
                [
                    'condition' => [
                        'dct:title.l' => ['$exists' => true],
                    ],
                ],
            ],
            'indices' => [
                [
                    'fieldName' => 'search_terms',
                    'predicates' => ['dct:title'],
                ],
            ],
            'fields' => [
                [
                    'fieldName' => 'result.title',
                    'predicates' => ['dct:title'],
                    'limit' => 1,
                ],
                [
                    'fieldName' => 'result.date',
                    'predicates' => ['dct:date'],
                    'limit' => 1,
                ],
                [
                    'fieldName' => 'result.price',
                    'predicates' => ['temp:price'],
                    'limit' => 1,
                ],
                [
                    'fieldName' => 'result.isHardcover',
                    'predicates' => ['temp:isHardcover'],
                    'limit' => 1,
                ],
            ],
        ];

        $graph = new MongoGraph();
        $graph->add_tripod_array($doc);
        $this->tripod->saveChanges(new ExtendedGraph(), $graph, $doc['_id'][_ID_CONTEXT]);

        $mockSearchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();

        $mockSearchDocuments->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->willReturn($searchSpecs);

        $generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/typedDoc1', 'http://talisaspire.com/');
        $this->assertNotNull($generatedDocuments);
        $this->assertEquals([
            'title' => 'Typed PHP: Stronger Types For Cleaner Code',
            'date' => '2016',
            'price' => '19.99',
            'isHardcover' => '1',
        ], $generatedDocuments['result']);
    }

    public function testGenerateSearchDocumentBasedOnSpecIdWithFieldNamePredicatesHavingNoValueInCollection(): void
    {
        $searchSpecs = json_decode(
            '{"_id":"i_search_resource","type":["bibo:Book"],"from":"CBD_testing","filter":[{"condition":{"dct:title.l":{"$exists":true}}}],"indices":[{"fieldName":"search_terms","predicates":["dct:title","dct:subject"]},{"fieldName":"other_terms","predicates":["rdf:type"]}],"fields":[{"fieldName":"result.title","predicates":["dct:title"],"limit":1},{"fieldName":"result.link","value":"link"},{"fieldName":"rdftype","predicates":["rdf:type"],"limit":1}],"joins":{"dct:creator":{"indices":[{"fieldName":"search_terms","predicates":["foaf:name"]}],"fields":[{"fieldName":"result.author","predicates":["foaf:name"],"limit":1}, {"fieldName":"result.role","predicates":["siocAccess:Role"], "limit":1}] } }}',
            true
        );

        $mockSearchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['getSearchDocumentSpecification'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();

        $mockSearchDocuments->expects($this->once())
            ->method('getSearchDocumentSpecification')
            ->willReturn($searchSpecs);

        $generatedDocuments = $mockSearchDocuments->generateSearchDocumentBasedOnSpecId('i_search_resource', 'http://talisaspire.com/resources/doc1', 'http://talisaspire.com/');
        $this->assertNotNull($generatedDocuments);
        $this->assertEquals('http://talisaspire.com/resources/doc1', $generatedDocuments['_id']['r']);
    }

    public function testSearchDocumentsGenerateWhenDefinedPredicateChanges(): void
    {
        $uri = 'http://talisaspire.com/resources/doc1';

        $labeller = new Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:subject'],
        ];

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchDocumentGenerator'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();

        $searchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['generateSearchDocumentBasedOnSpecId'])
            ->setConstructorArgs(
                [
                    $this->defaultStoreName,
                    Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext,
                ]
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->willReturn($searchDocuments);

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_resource', $labeller->uri_to_alias($uri), $this->defaultContext);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uri,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['i_search_resource']
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $searchIndexer->update($subject);
        }
    }

    public function testSearchDocsShouldRegenerateWhenUndefinedPredicateChangesButFilterExistsInSpec(): void
    {
        $uri = 'http://talisaspire.com/resources/doc1';

        $labeller = new Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:description'],
        ];

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($uri, $this->defaultContext, $this->defaultPodName);
        $impactedSubjects = $this->tripod->getSearchIndexer()->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertCount(1, $impactedSubjects);
        $this->assertEquals(
            [
                _ID_RESOURCE => $uri,
                _ID_CONTEXT => 'http://talisaspire.com/',
            ],
            $impactedSubjects[0]->getResourceId()
        );

        $this->assertEmpty($impactedSubjects[0]->getSpecTypes());
    }

    public function testUpdateOfResourceInImpactIndexTriggersRegenerationOfSearchDocs(): void
    {
        $uri = 'http://talisaspire.com/authors/2';
        $labeller = new Labeller();

        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            'http://talisaspire.com/resources/doc4',
            $this->defaultContext,
            $this->defaultPodName
        );

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['foaf:name'],
        ];

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchDocumentGenerator'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();

        $searchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['generateSearchDocumentBasedOnSpecId'])
            ->setConstructorArgs(
                [
                    $this->defaultStoreName,
                    Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext,
                ]
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->willReturn($searchDocuments);

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_resource', $labeller->uri_to_alias('http://talisaspire.com/resources/doc4'), $this->defaultContext);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://talisaspire.com/resources/doc4',
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['i_search_resource']
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $searchIndexer->update($subject);
        }
    }

    public function testRdfTypeTriggersGenerationOfSearchDocuments(): void
    {
        $uri = 'http://example.com/resources/' . uniqid();

        $labeller = new Labeller();
        $graph = new ExtendedGraph();
        // This should trigger a search document regeneration, even though issn isn't in the search doc spec
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('baseData:Wibble'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('bibo:issn'), '1234-5678');

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => [
                'rdf:type',
                'bibo:issn',
            ],
        ];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchDocumentGenerator'])
            ->setConstructorArgs([$this->tripod])
            ->getMock();

        $searchDocuments = $this->getMockBuilder(SearchDocuments::class)
            ->onlyMethods(['generateSearchDocumentBasedOnSpecId'])
            ->setConstructorArgs(
                [
                    $this->defaultStoreName,
                    Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                    $this->defaultContext,
                ]
            )->getMock();

        $searchIndexer->expects($this->once())
            ->method('getSearchDocumentGenerator')
            ->willReturn($searchDocuments);

        $searchDocuments->expects($this->once())
            ->method('generateSearchDocumentBasedOnSpecId')
            ->with('i_search_filter_parse', $labeller->uri_to_alias($uri), $this->defaultContext);

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $impactedSubjects = $searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri),
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                []
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $searchIndexer->update($subject);
        }
    }

    public function testNewResourceThatDoesNotMatchAnythingCreatesNoImpactedSubjects(): void
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('bibo:Proceedings'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('dct:title'), 'A title');

        $subjectsAndPredicatesOfChange = [$uriAlias => ['rdf:type', 'dct:title']];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $searchIndexer = $mockTripod->getComposite(OP_SEARCH);

        $this->assertEmpty($searchIndexer->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

    public function testDeleteResourceCreatesImpactedSubjects(): void
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $creatorUri = 'http://example.com/identities/oscar-wilde';
        $creatorUriAlias = $labeller->uri_to_alias($creatorUri);

        $graph = new ExtendedGraph();
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('acorn:Resource')
        );
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('bibo:Book')
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('dct:title'),
            'The Importance of Being Earnest'
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('dct:subject'),
            'Plays -- Satire'
        );
        $graph->add_resource_triple(
            $uri,
            $labeller->qname_to_uri('dct:creator'),
            $creatorUri
        );

        $uri2 = 'http://example.com/resources/' . uniqid();
        $uriAlias2 = $labeller->uri_to_alias($uri2);

        $graph2 = new ExtendedGraph();
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('acorn:Resource')
        );
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('bibo:Book')
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('dct:title'),
            'The Picture of Dorian Gray'
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('dct:subject'),
            'Portraits -- Fiction'
        );
        $graph2->add_resource_triple(
            $uri2,
            $labeller->qname_to_uri('dct:creator'),
            $creatorUri
        );

        $graph3 = new ExtendedGraph();
        $graph3->add_resource_triple(
            $creatorUri,
            RDF_TYPE,
            $labeller->qname_to_uri('foaf:Person')
        );
        $graph3->add_literal_triple(
            $creatorUri,
            $labeller->qname_to_uri('foaf:name'),
            'Oscar Wilde'
        );

        // Save the graphs and ensure that table rows are generated
        $tripod = new Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            [
                'defaultContext' => $this->defaultContext,
                OP_ASYNC => [
                    OP_VIEWS => false,
                    OP_TABLES => false,
                    OP_SEARCH => false,
                ],
            ]
        );

        // Save the author graph first so the joins work
        $tripod->saveChanges(new ExtendedGraph(), $graph3);

        $tripod->saveChanges(new ExtendedGraph(), $graph);

        $collection = Config::getInstance()->getCollectionForSearchDocument($this->defaultStoreName, 'i_search_resource');

        $query = [
            _ID_KEY => [
                _ID_RESOURCE => $uriAlias,
                _ID_CONTEXT => $this->defaultContext,
                _ID_TYPE => 'i_search_resource',
            ],
        ];
        $this->assertEquals(1, $collection->count($query));

        $tripod->saveChanges(new ExtendedGraph(), $graph2);

        $query[_ID_KEY][_ID_RESOURCE] = $uriAlias2;
        $this->assertEquals(1, $collection->count($query));

        $impactQuery = [
            _ID_KEY . '.' . _ID_TYPE => 'i_search_resource',
            '_impactIndex' => [
                _ID_RESOURCE => $creatorUriAlias,
                _ID_CONTEXT => $this->defaultContext,
            ],
            'result.author' => 'Oscar Wilde',
        ];
        $this->assertEquals(2, $collection->count($impactQuery));

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    $this->defaultPodName,
                    $this->defaultStoreName,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->setConstructorArgs(
                [
                    $mockTripod,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->onlyMethods(['processSyncOperations'])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $expectedSubjectsAndPredicatesOfChange = [
            $creatorUriAlias => ['rdf:type', 'foaf:name'],
        ];

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $expectedSubjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        // Delete creator resource
        $mockTripod->saveChanges($graph3, new ExtendedGraph());

        $deletedGraph = $mockTripod->describeResource($creatorUri);
        $this->assertTrue($deletedGraph->is_empty());

        // Manually walk through the tables operation
        $search = $mockTripod->getComposite(OP_SEARCH);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['i_search_resource']
            ),
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias2,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_SEARCH,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['i_search_resource']
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $search->getImpactedSubjects($expectedSubjectsAndPredicatesOfChange, $this->defaultContext));

        foreach ($expectedImpactedSubjects as $subject) {
            $search->update($subject);
        }

        $query = [
            _ID_KEY => [
                _ID_RESOURCE => $uriAlias,
                _ID_CONTEXT => $this->defaultContext,
                _ID_TYPE => 'i_search_resource',
            ],
        ];
        $this->assertEquals(1, $collection->count($query));

        $query[_ID_KEY][_ID_RESOURCE] = $uriAlias2;
        $this->assertEquals(1, $collection->count($query));

        // Deleted resource will still be impact indexes because join still exists
        $impactQuery = [
            _ID_KEY . '.' . _ID_TYPE => 'i_search_resource',
            '_impactIndex' => [
                _ID_RESOURCE => $creatorUriAlias,
                _ID_CONTEXT => $this->defaultContext,
            ],
        ];
        $this->assertEquals(2, $collection->count($impactQuery));

        // But the document should have been regenerated without the value
        $impactQuery['result.author'] = 'Oscar Wilde';
        $this->assertEquals(0, $collection->count($impactQuery));
    }

    private function getSearchDocuments(Driver $tripod): SearchDocuments
    {
        return new SearchDocuments(
            $tripod->getStoreName(),
            $this->getTripodCollection($tripod),
            'http://talisaspire.com/'
        );
    }
}
