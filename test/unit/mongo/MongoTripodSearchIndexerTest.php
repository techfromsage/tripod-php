<?php

class MongoTripodSearchIndexerTest extends MongoTripodTestBase {

    protected function setUp(): void
    {
        parent::setUp();

        $this->tripod = new \Tripod\Mongo\Driver("CBD_testing", "tripod_php_testing", array("async"=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>false)));
        foreach (\Tripod\Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }
        $this->loadResourceDataViaTripod();
        $this->loadBaseSearchDataViaTripod();
    }

    public function testSearchDocumentsRegenerateWhenDefinedPredicateChanged()
    {
        // First make a change that affects a search document
        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(array('getSearchIndexer', 'getDataUpdater'))
            ->setConstructorArgs(array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(\Tripod\Mongo\Updates::class)
            ->onlyMethods(array('storeChanges'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();

        $labeller = new \Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias("http://talisaspire.com/authors/1")=>array("foaf:name")
        );

        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->will($this->returnValue(array("subjectsAndPredicatesOfChange"=>$subjectsAndPredicatesOfChange,"transaction_id"=>"t1234")));

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $searchIndexer = $this->getMockBuilder(\Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(array('getSearchProvider', 'getImpactedSubjects'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider = $this->getMockBuilder(\Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(array('deleteDocument','indexDocument'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider->expects($this->exactly(3))
            ->method('deleteDocument')
            ->with(
                $this->matchesRegularExpression("/http:\/\/talisaspire\.com\/resources\/doc(1|2|3)$/"),
                'http://talisaspire.com/',
                $this->equalTo(array('i_search_resource')))
        ;

        $searchProvider->expects($this->exactly(3))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->will($this->returnValue($searchProvider));

        $impactedSubjects = array(
            $this->getMockBuilder(\Tripod\Mongo\ImpactedSubject::class)
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://talisaspire.com/resources/doc1',
                            _ID_CONTEXT=>'http://talisaspire.com/',
                        ),
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        array('i_search_resource')
                    )
                )
                ->onlyMethods(array('getTripod'))
                ->getMock(),
            $this->getMockBuilder(\Tripod\Mongo\ImpactedSubject::class)
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://talisaspire.com/resources/doc2',
                            _ID_CONTEXT=>'http://talisaspire.com/',
                        ),
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        array('i_search_resource')
                    )
                )
                ->onlyMethods(array('getTripod'))
                ->getMock(),
            $this->getMockBuilder(\Tripod\Mongo\ImpactedSubject::class)
                ->setConstructorArgs(
                    array(
                        array(
                            _ID_RESOURCE=>'http://talisaspire.com/resources/doc3',
                            _ID_CONTEXT=>'http://talisaspire.com/',
                        ),
                        OP_SEARCH,
                        'tripod_php_testing',
                        'CBD_testing',
                        array('i_search_resource')
                    )
                )
                ->onlyMethods(array('getTripod'))
                ->getMock()
        );

        $impactedSubjects[0]->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));
        $impactedSubjects[1]->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));
        $impactedSubjects[2]->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));

        $searchIndexer->expects($this->once())
            ->method('getImpactedSubjects')
            ->with($subjectsAndPredicatesOfChange, 'http://talisaspire.com/')
            ->will($this->returnValue($impactedSubjects));


        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $g1 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2->add_literal_triple("http://talisaspire.com/authors/1", $g2->qname_to_uri("foaf:name"),"Bill Shakespeare" );

        $tripod->saveChanges($g1, $g2);

        // Now make a change that affects a different search document - Create new document
        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(array('getSearchIndexer'))
            ->setConstructorArgs(array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();

        $searchIndexer = $this->getMockBuilder(\Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(array('getSearchProvider', 'getImpactedSubjects'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider = $this->getMockBuilder(\Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(array('deleteDocument','indexDocument'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider->expects($this->exactly(1))
            ->method('deleteDocument')
            ->with(
                $this->equalTo("http://talisaspire.com/lists/1234"),
                'http://talisaspire.com/',
                $this->isEmpty())
        ;

        $searchProvider->expects($this->exactly(1))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->will($this->returnValue($searchProvider));

        $impactedSubject = $this->getMockBuilder(\Tripod\Mongo\ImpactedSubject::class)
            ->setConstructorArgs(
                array(
                    array(
                        _ID_RESOURCE=>'http://talisaspire.com/lists/1234',
                        _ID_CONTEXT=>'http://talisaspire.com/',
                    ),
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing'
                )
            )
            ->onlyMethods(array('getTripod'))
            ->getMock();

        $impactedSubject->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));

        $searchIndexer->expects($this->once())->method('getImpactedSubjects')->will($this->returnValue(array($impactedSubject)));


        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $list = new \Tripod\ExtendedGraph();
        $list->add_resource_triple("http://talisaspire.com/lists/1234", RDF_TYPE, "http://purl.org/vocab/resourcelist/schema#List");
        $list->add_literal_triple("http://talisaspire.com/lists/1234", "http://rdfs.org/sioc/spec/name", "Testing list");

        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $list);

        // Regen our search docs for real since this step was overridden in the stub
        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            "http://talisaspire.com/lists/1234",
            "http://talisaspire.com/",
            'CBD_testing'
        );

        // Now make a change to the last document
        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(array('getSearchIndexer'))
            ->setConstructorArgs(array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();

        $searchIndexer = $this->getMockBuilder(\Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(array('getSearchProvider', 'getImpactedSubjects'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider = $this->getMockBuilder(\Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(array('deleteDocument','indexDocument'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $impactedSubject = $this->getMockBuilder(\Tripod\Mongo\ImpactedSubject::class)
            ->setConstructorArgs(
                array(
                    array(
                        _ID_RESOURCE=>'http://talisaspire.com/lists/1234',
                        _ID_CONTEXT=>'http://talisaspire.com/',
                    ),
                    OP_SEARCH,
                    'tripod_php_testing',
                    'CBD_testing',
                    array('i_search_list')
                )
            )
            ->onlyMethods(array('getTripod'))
            ->getMock();

        $impactedSubject->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));

        $searchProvider->expects($this->exactly(1))
            ->method('deleteDocument')
            ->with(
                $this->equalTo("http://talisaspire.com/lists/1234"),
                'http://talisaspire.com/',
                array('i_search_list')
            );

        $searchProvider->expects($this->exactly(1))
            ->method('indexDocument');

        $searchIndexer->expects($this->atLeastOnce())
            ->method('getSearchProvider')
            ->will($this->returnValue($searchProvider));

        $searchIndexer->expects($this->once())->method('getImpactedSubjects')->will($this->returnValue(array($impactedSubject)));

        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $oldList = $tripod->describeResource("http://talisaspire.com/lists/1234");
        $list = $tripod->describeResource("http://talisaspire.com/lists/1234");
        $list->remove_property_values("http://talisaspire.com/lists/1234", "http://rdfs.org/sioc/spec/name");
        $list->add_literal_triple("http://talisaspire.com/lists/1234", "http://rdfs.org/sioc/spec/name", "IMPROVED testing list");

        $tripod->saveChanges($oldList, $list);

        // Regen our search docs for real since this step was overridden in the stub again
        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            "http://talisaspire.com/lists/1234",
            "http://talisaspire.com/",
            'CBD_testing'
        );
    }

    function testSearchDocumentsNotRegeneratedIfChangeIsNotInSearchSpec()
    {

        // Now make a change that shouldn't affect any search docs
        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(array('getSearchIndexer', 'getDataUpdater'))
            ->setConstructorArgs(array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();

        $tripodUpdate = $this->getMockBuilder(\Tripod\Mongo\Updates::class)
            ->onlyMethods(array('storeChanges'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            ))
            ->getMock();
        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->will($this->returnValue(array('deletedSubjects'=>array(),"subjectsAndPredicatesOfChange"=>array(),"transaction_id"=>'t1234')));

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $searchIndexer = $this->getMockBuilder(\Tripod\Mongo\Composites\SearchIndexer::class)
            ->onlyMethods(array('getSearchProvider', 'update'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider = $this->getMockBuilder(\Tripod\Mongo\MongoSearchProvider::class)
            ->onlyMethods(array('deleteDocument','indexDocument'))
            ->setConstructorArgs(array($tripod))
            ->getMock();

        $searchProvider->expects($this->never())
            ->method('deleteDocument');

        $searchProvider->expects($this->never())
            ->method('indexDocument');

        $searchIndexer->expects($this->any())
            ->method('getSearchProvider')
            ->will($this->returnValue($searchProvider));

        $searchIndexer->expects($this->never())
            ->method('update');



        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $g1 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2->add_literal_triple("http://talisaspire.com/authors/1", $g2->qname_to_uri("foaf:dob"),"1564-04-26" );
        $tripod->saveChanges($g1, $g2);
    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject()
    {
        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(array('getDataUpdater'))
            ->setConstructorArgs(
                array(
                    'CBD_testing',
                    'tripod_php_testing',
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(
                            OP_VIEWS=>true,
                            OP_TABLES=>true,
                            OP_SEARCH=>true
                        )
                    )
                )
            )->getMock();

        $tripodUpdates = $this->getMockBuilder(\Tripod\Mongo\Updates::class)
            ->onlyMethods(array())
            ->setConstructorArgs(
                array(
                    $tripod,
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(
                            OP_VIEWS=>true,
                            OP_TABLES=>true,
                            OP_SEARCH=>true
                        )
                    )
                )
            )->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new \Tripod\Mongo\MongoGraph();
        $newSubjectUri1 = "http://talisaspire.com/resources/newdoc1";
        $newSubjectUri2 = "http://talisaspire.com/resources/newdoc2";
        $newSubjectUri3 = "http://talisaspire.com/resources/newdoc3";

        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("bibo:Article")); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($newSubjectUri1,  $g->qname_to_uri("dct:title"),   "This is a new resource");
        $g->add_literal_triple($newSubjectUri1,  $g->qname_to_uri("dct:subject"), "history");
        $g->add_literal_triple($newSubjectUri1,  $g->qname_to_uri("dct:subject"), "philosophy");

        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("bibo:Book")); // this is the only resource that should be queued
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("acorn:Resource"));
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($newSubjectUri2,  $g->qname_to_uri("dct:title"),   "This is another new resource");
        $g->add_literal_triple($newSubjectUri2,  $g->qname_to_uri("dct:subject"), "maths");
        $g->add_literal_triple($newSubjectUri2,  $g->qname_to_uri("dct:subject"), "science");

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("bibo:Journal")); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:title"),   "This is yet another new resource");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "art");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "design");
        $subjectsAndPredicatesOfChange = array(
            $newSubjectUri1=>array('rdf:type','dct:creator','dct:title','dct:subject'),
            $newSubjectUri2=>array('rdf:type','dct:creator','dct:title','dct:subject'),
            $newSubjectUri3=>array('rdf:type','dct:creator','dct:title','dct:subject')
        );
        $tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g);

        $search = $tripod->getComposite(OP_SEARCH);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$newSubjectUri2,
                    _ID_CONTEXT=>'http://talisaspire.com/'
                ),
                OP_SEARCH,
                'tripod_php_testing',
                'CBD_testing',
                array()
            )
        );

        $impactedSubjects = $search->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testBatchSearchDocumentsGeneration()
    {
        $count = 234;
        $docs = [];

        $configOptions = json_decode(file_get_contents(__DIR__ . '/data/config.json'), true);

        for ($i = 0; $i < $count; $i++) {
            $docs[] = ['_id' => ['r' => 'tenantLists:batch' . $i, 'c' => 'tenantContexts:DefaultGraph']];
        }

        $fakeCursor = new ArrayIterator($docs);
        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig($configOptions);

        $collection = $this->getMockBuilder(\MongoDB\Collection::class)
            ->onlyMethods(['count', 'find'])
            ->setConstructorArgs([new \MongoDB\Driver\Manager(), 'db', 'coll'])
            ->getMock();
        $collection->expects($this->atLeastOnce())->method('count')->willReturn($count);
        $collection->expects($this->atLeastOnce())->method('find')->willReturn($fakeCursor);

        $jobGroup = $this->getMockBuilder(\Tripod\Mongo\JobGroup::class)
            ->onlyMethods(['setJobCount'])
            ->setConstructorArgs(['tripod_php_testing'])
            ->getMock();
        $jobGroup->expects($this->once())->method('setJobCount')->with($count);

        $configInstance->expects($this->atLeastOnce())->method('getCollectionForCBD')->willReturn($collection);

        $tripod = $this->getMockBuilder(\Tripod\Mongo\Driver::class)
            ->onlyMethods(['getConfigInstance',])
            ->setConstructorArgs(['tripod_php_testing', 'CBD_testing'])
            ->disableOriginalConstructor()
            ->getMock();

        $search = $this->getMockBuilder(\Tripod\Mongo\Composites\SearchIndexer::class)
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
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array')
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array')
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(34)
                    ),
                    'TESTQUEUE',
                    $this->isType('array')
                ]
            );
        $search->generateSearchDocuments('i_search_list', null, null, 'TESTQUEUE');
    }
}
