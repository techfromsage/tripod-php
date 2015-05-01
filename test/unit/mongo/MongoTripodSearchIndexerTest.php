<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';
require_once 'src/mongo/MongoGraph.class.php';


class MongoTripodSearchIndexerTest extends MongoTripodTestBase {

    protected function setUp()
    {
        parent::setUp();

        $this->tripod = new MongoTripod("CBD_testing", "tripod_php_testing", array("async"=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>false)));
        foreach(MongoTripodConfig::getInstance()->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
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
            'MongoTripod',
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
            'MongoTripodUpdates',
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

        // @todo: how did this trigger anything in the first place?!
        $tripodUpdate->expects($this->atLeastOnce())
            ->method('storeChanges')
            ->will($this->returnValue(array('deletedSubjects'=>array())));

        $tripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdate));

        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
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
            'MongoTripod',
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

        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
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

        $list = new ExtendedGraph();
        $list->add_resource_triple("http://talisaspire.com/lists/1234", RDF_TYPE, "http://purl.org/vocab/resourcelist/schema#List");
        $list->add_literal_triple("http://talisaspire.com/lists/1234", "http://rdfs.org/sioc/spec/name", "Testing list");

        $tripod->saveChanges(new ExtendedGraph(), $list);

        // Regen our search docs for real since this step was overridden in the stub
        $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments(
            "http://talisaspire.com/lists/1234",
            "http://talisaspire.com/",
            'CBD_testing'
        );

        // Now make a change to the last document
        $tripod = $this->getMock(
            'MongoTripod',
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

        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
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
            'MongoTripod',
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
            'MongoTripodUpdates',
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

        $searchIndexer = $this->getMock('MongoTripodSearchIndexer',
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

}