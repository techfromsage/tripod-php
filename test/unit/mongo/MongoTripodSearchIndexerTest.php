<?php

declare(strict_types=1);

use MongoDB\Collection;
use MongoDB\Driver\Manager;
use Tripod\Config;
use Tripod\ExtendedGraph;
use Tripod\Mongo\Composites\SearchIndexer;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\JobGroup;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\MongoSearchProvider;
use Tripod\Mongo\Updates;
use Tripod\Test\Mongo\Mocks\Cursor as FakeCursor;

class MongoTripodSearchIndexerTest extends MongoTripodTestBase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing', ['async' => [OP_VIEWS => true, OP_TABLES => true, OP_SEARCH => false]]);
        foreach (Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }

        $this->loadResourceDataViaTripod();
        $this->loadBaseSearchDataViaTripod();
    }

    public function testSearchDocumentsRegenerateWhenDefinedPredicateChanged(): void
    {
        // First make a change that affects a search document
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getSearchIndexer', 'getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['storeChanges'])
            ->setConstructorArgs([
                $tripod,
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $labeller = new Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://talisaspire.com/authors/1') => ['foaf:name'],
        ];

        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->willReturn(['subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange, 'transaction_id' => 't1234']);

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchProvider', 'getImpactedSubjects'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider = $this->getMockBuilder(MongoSearchProvider::class)
            ->onlyMethods(['deleteDocument', 'indexDocument'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider->expects($this->exactly(3))
            ->method('deleteDocument')
            ->with(
                $this->matchesRegularExpression('/http:\/\/talisaspire\.com\/resources\/doc(1|2|3)$/'),
                'http://talisaspire.com/',
                ['i_search_resource']
            );

        $searchProvider->expects($this->exactly(3))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->willReturn($searchProvider);

        $impactedSubjects = [
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://talisaspire.com/resources/doc1',
                            _ID_CONTEXT => 'http://talisaspire.com/',
                        ],
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        ['i_search_resource'],
                    ]
                )
                ->onlyMethods(['getTripod'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://talisaspire.com/resources/doc2',
                            _ID_CONTEXT => 'http://talisaspire.com/',
                        ],
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        ['i_search_resource'],
                    ]
                )
                ->onlyMethods(['getTripod'])
                ->getMock(),
            $this->getMockBuilder(ImpactedSubject::class)
                ->setConstructorArgs(
                    [
                        [
                            _ID_RESOURCE => 'http://talisaspire.com/resources/doc3',
                            _ID_CONTEXT => 'http://talisaspire.com/',
                        ],
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        ['i_search_resource'],
                    ]
                )
                ->onlyMethods(['getTripod'])
                ->getMock(),
        ];

        $impactedSubjects[0]->expects($this->once())->method('getTripod')->willReturn($tripod);
        $impactedSubjects[1]->expects($this->once())->method('getTripod')->willReturn($tripod);
        $impactedSubjects[2]->expects($this->once())->method('getTripod')->willReturn($tripod);

        $searchIndexer->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($subjectsAndPredicatesOfChange, 'http://talisaspire.com/')
            ->willReturn($impactedSubjects);

        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->willReturn($searchIndexer);

        $g1 = $tripod->describeResource('http://talisaspire.com/authors/1');
        $g2 = $tripod->describeResource('http://talisaspire.com/authors/1');
        $g2->add_literal_triple('http://talisaspire.com/authors/1', $g2->qname_to_uri('foaf:name'), 'Bill Shakespeare');

        $tripod->saveChanges($g1, $g2);

        // Now make a change that affects a different search document - Create new document
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getSearchIndexer'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchProvider', 'getImpactedSubjects'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider = $this->getMockBuilder(MongoSearchProvider::class)
            ->onlyMethods(['deleteDocument', 'indexDocument'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider->expects($this->exactly(1))
            ->method('deleteDocument')
            ->with(
                'http://talisaspire.com/lists/1234',
                'http://talisaspire.com/',
                $this->isEmpty()
            );

        $searchProvider->expects($this->exactly(1))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->willReturn($searchProvider);

        $impactedSubject = $this->getMockBuilder(ImpactedSubject::class)
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://talisaspire.com/lists/1234',
                        _ID_CONTEXT => 'http://talisaspire.com/',
                    ],
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                ]
            )
            ->onlyMethods(['getTripod'])
            ->getMock();

        $impactedSubject->expects($this->once())->method('getTripod')->willReturn($tripod);

        $searchIndexer->expects($this->once())->method('getImpactedSubjects')->willReturn([$impactedSubject]);

        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->willReturn($searchIndexer);

        $list = new ExtendedGraph();
        $list->add_resource_triple('http://talisaspire.com/lists/1234', RDF_TYPE, 'http://purl.org/vocab/resourcelist/schema#List');
        $list->add_literal_triple('http://talisaspire.com/lists/1234', 'http://rdfs.org/sioc/spec/name', 'Testing list');

        $tripod->saveChanges(new ExtendedGraph(), $list);

        // Regen our search docs for real since this step was overridden in the stub
        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            'http://talisaspire.com/lists/1234',
            'http://talisaspire.com/',
            'CBD_testing'
        );

        // Now make a change to the last document
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getSearchIndexer'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchProvider', 'getImpactedSubjects'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider = $this->getMockBuilder(MongoSearchProvider::class)
            ->onlyMethods(['deleteDocument', 'indexDocument'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $impactedSubject = $this->getMockBuilder(ImpactedSubject::class)
            ->setConstructorArgs(
                [
                    [
                        _ID_RESOURCE => 'http://talisaspire.com/lists/1234',
                        _ID_CONTEXT => 'http://talisaspire.com/',
                    ],
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                    ['i_search_list'],
                ]
            )
            ->onlyMethods(['getTripod'])
            ->getMock();

        $impactedSubject->expects($this->once())->method('getTripod')->willReturn($tripod);

        $searchProvider->expects($this->exactly(1))
            ->method('deleteDocument')
            ->with(
                'http://talisaspire.com/lists/1234',
                'http://talisaspire.com/',
                ['i_search_list']
            );

        $searchProvider->expects($this->exactly(1))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->willReturn($searchProvider);

        $searchIndexer->expects($this->once())->method('getImpactedSubjects')->willReturn([$impactedSubject]);

        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->willReturn($searchIndexer);

        $oldList = $tripod->describeResource('http://talisaspire.com/lists/1234');
        $list = $tripod->describeResource('http://talisaspire.com/lists/1234');
        $list->remove_property_values('http://talisaspire.com/lists/1234', 'http://rdfs.org/sioc/spec/name');
        $list->add_literal_triple('http://talisaspire.com/lists/1234', 'http://rdfs.org/sioc/spec/name', 'IMPROVED testing list');

        $tripod->saveChanges($oldList, $list);

        // Regen our search docs for real since this step was overridden in the stub again
        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            'http://talisaspire.com/lists/1234',
            'http://talisaspire.com/',
            'CBD_testing'
        );
    }

    public function testSearchDocumentsNotRegeneratedIfChangeIsNotInSearchSpec(): void
    {
        // Now make a change that shouldn't affect any search docs
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getSearchIndexer', 'getDataUpdater'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(Updates::class)
            ->onlyMethods(['storeChanges'])
            ->setConstructorArgs([
                $tripod,
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => true,
                        OP_SEARCH => false,
                    ],
                ],
            ])
            ->getMock();
        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->willReturn(['deletedSubjects' => [], 'subjectsAndPredicatesOfChange' => [], 'transaction_id' => 't1234']);

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdate);

        $searchIndexer = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['getSearchProvider', 'update'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider = $this->getMockBuilder(MongoSearchProvider::class)
            ->onlyMethods(['deleteDocument', 'indexDocument'])
            ->setConstructorArgs([$tripod])
            ->getMock();

        $searchProvider->expects($this->never())
            ->method('deleteDocument');

        $searchProvider->expects($this->never())
            ->method('indexDocument');

        $searchIndexer
            ->method('getSearchProvider')
            ->willReturn($searchProvider);

        $searchIndexer->expects($this->never())
            ->method('update');

        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->willReturn($searchIndexer);

        $g1 = $tripod->describeResource('http://talisaspire.com/authors/1');
        $g2 = $tripod->describeResource('http://talisaspire.com/authors/1');
        $g2->add_literal_triple('http://talisaspire.com/authors/1', $g2->qname_to_uri('foaf:dob'), '1564-04-26');

        $tripod->saveChanges($g1, $g2);
    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created.
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject(): void
    {
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    'CBD_testing',
                    'tripod_php_testing',
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([])
            ->setConstructorArgs(
                [
                    $tripod,
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdates);

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $newSubjectUri1 = 'http://talisaspire.com/resources/newdoc1';
        $newSubjectUri2 = 'http://talisaspire.com/resources/newdoc2';
        $newSubjectUri3 = 'http://talisaspire.com/resources/newdoc3';

        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Article')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:title'), 'This is a new resource');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'history');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'philosophy');

        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Book')); // this is the only resource that should be queued
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:title'), 'This is another new resource');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'maths');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'science');

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Journal')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:title'), 'This is yet another new resource');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'art');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'design');

        $subjectsAndPredicatesOfChange = [
            $newSubjectUri1 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri2 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri3 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
        ];
        $tripod->saveChanges(new MongoGraph(), $g);

        $search = $tripod->getComposite(OP_SEARCH);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $newSubjectUri2,
                    _ID_CONTEXT => 'http://talisaspire.com/',
                ],
                OP_SEARCH,
                'tripod_php_testing',
                'CBD_testing',
                []
            ),
        ];

        $impactedSubjects = $search->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testBatchSearchDocumentsGeneration(): void
    {
        $count = 234;
        $docs = [];

        $configOptions = $this->decodeJsonFile(__DIR__ . '/data/config.json');

        for ($i = 0; $i < $count; $i++) {
            $docs[] = ['_id' => ['r' => 'tenantLists:batch' . $i, 'c' => 'tenantContexts:DefaultGraph']];
        }

        $fakeCursor = new FakeCursor($docs);
        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig($configOptions);

        $collection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['count', 'find'])
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->getMock();
        $collection->expects($this->atLeastOnce())->method('count')->willReturn($count);
        $collection->expects($this->atLeastOnce())->method('find')->willReturn($fakeCursor);

        $jobGroup = $this->getMockBuilder(JobGroup::class)
            ->onlyMethods(['setJobCount'])
            ->setConstructorArgs(['tripod_php_testing'])
            ->getMock();
        $jobGroup->expects($this->once())->method('setJobCount')->with($count);

        $configInstance->expects($this->atLeastOnce())->method('getCollectionForCBD')->willReturn($collection);

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getStoreName'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();
        $tripod->expects($this->atLeastOnce())->method('getStoreName')->willReturn('tripod_php_testing');

        $search = $this->getMockBuilder(SearchIndexer::class)
            ->onlyMethods(['setSearchProvider', 'getConfigInstance', 'queueApplyJob', 'getJobGroup'])
            ->setConstructorArgs([$tripod])
            ->getMock();
        $search->expects($this->atLeastOnce())->method('getConfigInstance')->willReturn($configInstance);
        $search->expects($this->once())->method('getJobGroup')->willReturn($jobGroup);
        $search->expects($this->exactly(3))->method('queueApplyJob')
            ->withConsecutive(
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(34)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ]
            );
        $search->generateSearchDocuments('i_search_list', null, null, 'TESTQUEUE');
    }
}
