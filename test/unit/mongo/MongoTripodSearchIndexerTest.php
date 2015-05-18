<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/Tripod.class.php';
require_once 'src/mongo/MongoGraph.class.php';


class MongoTripodSearchIndexerTest extends MongoTripodTestBase {

    protected function setUp()
    {
        parent::setUp();

        $this->tripod = new \Tripod\Mongo\Tripod("CBD_testing", "tripod_php_testing", array("async"=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>false)));
        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }
        $this->loadBaseDataViaTripod();
        $this->loadBaseSearchDataViaTripod();
    }

    public function testSearchDocumentsRegenerateWhenDefinedPredicateChanged()
    {
        // First make a change that affects a search document
        $tripod = $this->getMock(
            '\Tripod\Mongo\Tripod',
            array('getSearchIndexer', 'getDataUpdater'),
            array(
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
            )
        );

        $tripodUpdate = $this->getMock(
            '\Tripod\Mongo\Updates',
            array('storeChanges'),
            array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            )
        );

        $labeller = new \Tripod\Mongo\Labeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias("http://talisaspire.com/authors/1")=>array("foaf:name")
        );

        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->will($this->returnValue($subjectsAndPredicatesOfChange));

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $searchIndexer = $this->getMock('SearchIndexer',
            array('getSearchProvider'),
            array($tripod)
        );

        $searchProvider = $this->getMock('MongoSearchProvider',
            array('deleteDocument','indexDocument'),
            array($tripod)
        );

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


        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $g1 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2 = $tripod->describeResource("http://talisaspire.com/authors/1");
        $g2->add_literal_triple("http://talisaspire.com/authors/1", $g2->qname_to_uri("foaf:name"),"Bill Shakespeare" );

        $tripod->saveChanges($g1, $g2);

        // Now make a change that affects a different search document - Create new document
        $tripod = $this->getMock(
            'Tripod',
            array('getSearchIndexer'),
            array(
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
            )
        );

        $searchIndexer = $this->getMock('SearchIndexer',
            array('getSearchProvider'),
            array($tripod)
        );

        $searchProvider = $this->getMock('MongoSearchProvider',
            array('deleteDocument','indexDocument'),
            array($tripod)
        );

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
        $tripod = $this->getMock(
            '\Tripod\Mongo\Tripod',
            array('getSearchIndexer'),
            array(
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
            )
        );

        $searchIndexer = $this->getMock('SearchIndexer',
            array('getSearchProvider'),
            array($tripod)
        );

        $searchProvider = $this->getMock('MongoSearchProvider',
            array('deleteDocument','indexDocument'),
            array($tripod)
        );

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


        $tripod->expects($this->atLeastOnce())
            ->method('getSearchIndexer')
            ->will($this->returnValue($searchIndexer));

        $oldList = $tripod->describeResource("http://talisaspire.com/lists/1234");
        $list = $tripod->describeResource("http://talisaspire.com/lists/1234");
        /** @var ExtendedGraph $list */
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
        $tripod = $this->getMock(
            'Tripod',
            array('getSearchIndexer', 'getDataUpdater'),
            array(
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
            )
        );

        $tripodUpdate = $this->getMock(
            'Updates',
            array('storeChanges'),
            array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>true,
                        OP_SEARCH=>false
                    )
                )
            )
        );
        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->will($this->returnValue(array('deletedSubjects'=>array())));

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $searchIndexer = $this->getMock('SearchIndexer',
            array('getSearchProvider', 'update'),
            array($tripod)
        );

        $searchProvider = $this->getMock('MongoSearchProvider',
            array('deleteDocument','indexDocument'),
            array($tripod)
        );

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
        $tripod = $this->getMockBuilder('Tripod')
            ->setMethods(array('getDataUpdater'))
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

        $tripodUpdates = $this->getMockBuilder('Updates')
            ->setMethods(array('submitJob'))
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

        /** @var MongoTripodTables $tables */
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

}