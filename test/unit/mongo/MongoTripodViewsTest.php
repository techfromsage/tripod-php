<?php

require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/Tripod.class.php';
require_once 'src/mongo/delegates/Views.class.php';

class MongoTripodViewsTest extends MongoTripodTestBase {
    /**
     * @var Views
     */
    protected $tripodViews = null;

    private $viewsConstParams = null;
    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        /** @var Tripod|PHPUnit_Framework_MockObject_MockObject $this->tripod */
        $this->tripod = $this->getMock(
            'Tripod',
            array('addToSearchIndexQueue'),
            array('CBD_testing','tripod_php_testing',
                array(
                    "defaultContext"=>"http://talisaspire.com/",
                    "async"=>array(OP_VIEWS=>true)
                )
            ) // don't generate views syncronously when saving automatically - let unit tests deal with this
        );
        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->tripodViews = new \Tripod\Mongo\Views(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/'
        );


        foreach(\Tripod\Mongo\Config::getInstance()->getCollectionsForViews($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }

        $this->viewsConstParams = array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),'http://talisaspire.com/');

        // load base data
        $this->loadBaseDataViaTripod();
    }

    /**
     * Tests view spec properties include + join
     */
    public function testGenerateView()
    {
//        $this->tripodViews->generateView("v_resource_full","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA");
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource("http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","v_resource_full");

        $expectedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_full"),
            "value"=>array(
                _GRAPHS=>array(
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource")
                        ),
                        "searchterms:topic"=>array(
                            array(VALUE_LITERAL=>"engineering: general"),
                            array(VALUE_LITERAL=>"physics"),
                            array(VALUE_LITERAL=>"science")
                        ),
                        "dct:isVersionOf"=>array(VALUE_URI=>"http://talisaspire.com/works/4d101f63c10a6"),
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/4d101f63c10a6",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );
        // get the view direct from mongo
//        $result = $this->tripod->getViewForResource("http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","v_resource_full");
        $mongo = new MongoClient(\Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'));
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',"c"=>'http://talisaspire.com/',"type"=>'v_resource_full')));
        $this->assertEquals($expectedView,$actualView);
    }

    public function testGenerateViewWithTTL()
    {
        $expiryDate = new MongoDate(time()+300);
        $mockTripodViews = $this->getMock('Views', array('getExpirySecFromNow'), $this->viewsConstParams);
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time()+300)));

        $mockTripodViews->getViewForResource("http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","v_resource_full_ttl");

        // should have expires, but no impact index
        $expectedView = array(
            "_id"=>array(
                "r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA",
                "c"=>'http://talisaspire.com/',
                "type"=>"v_resource_full_ttl"),
            "value"=>array(
                _GRAPHS=>array(
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array("u"=>"bibo:Book"),
                            array("u"=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array("u"=>"bibo:Book"),
                            array("u"=>"acorn:Resource")
                        ),
                        "searchterms:topic"=>array(
                            array("l"=>"engineering: general"),
                            array("l"=>"physics"),
                            array("l"=>"science")
                        ),
                        "dct:isVersionOf"=>array("u"=>"http://talisaspire.com/works/4d101f63c10a6"),
                    )
                ),
                _EXPIRES=>$expiryDate
            )
        );
        // get the view direct from mongo
        $actualView = \Tripod\Mongo\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_full_ttl')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',"c"=>'http://talisaspire.com/',"type"=>'v_resource_full_ttl')));
        $this->assertEquals($expectedView,$actualView);
    }

    /**
     * This test covers a bug we found where maxJoins causes a difference between the URIs included in the right hand
     * side vs. the left hand side of the join. Consider the data:
     *
     * <s1> <p1> <o1>
     * <s1> <p1> <o2>
     * <o1> <p2> <o1o1>
     * <o2> <p2> <o2o1>
     *
     * Joining on p1 with a maxJoin of 1 you would expect this data in the view:
     *
     * <s1> <p1> <o1>
     * <o1> <p2> <o1o1>
     *
     * When in fact you can get:
     *
     * <s1> <p1> <o1>
     * <o2> <p2> <o2o1>
     *
     * Depending on the order the mongodriver selects data
     */
    public function testViewGenerationMaxJoinsObjectsMatchPredicates()
    {
        // get the view
        $graph = $this->tripodViews->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','v_resource_to_single_source');
        foreach ($graph->get_resource_triple_values('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','http://purl.org/dc/terms/source') as $object)
        {
            $this->assertFalse($graph->get_subject_subgraph($object)->is_empty(),"Subgraph for $object should not be empty, should have been followed as join");
        }
    }

    public function testTTLViewIsRegeneratedOnFetch()
    {
        // make mock return expiry date in past...
        $mockTripodViews = $this->getMock('Views', array('getExpirySecFromNow'), $this->viewsConstParams);
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time()-300)));

        $mockTripodViews->generateView("v_resource_full_ttl","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA");

        // now mock out generate views and check it's called...
        $mockTripodViews2 = $this->getMock('Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews2->expects($this->once())->method('generateView')->with('v_resource_full_ttl','http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $mockTripodViews2->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','v_resource_full_ttl');
    }

    public function testGenerateViewWithCountAggregate()
    {
        $expiryDate = new MongoDate(time()+300);
        /**
         * @var $mockTripodViews \Tripod\Mongo\Views
         */
        $mockTripodViews = $this->getMock('Views', array('getExpirySecFromNow'), $this->viewsConstParams);
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time()+300)));

        $mockTripodViews->getViewForResource("http://talisaspire.com/works/4d101f63c10a6","v_counts");

        $expectedView = array(
            "_id"=>array(
                "r"=>"http://talisaspire.com/works/4d101f63c10a6",
                "c"=>"http://talisaspire.com/",
                "type"=>"v_counts"),
            "value"=>array(
                _GRAPHS=>array(
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6-2","c"=>"http://talisaspire.com/"),
                        "rdf:type"=>array(
                            array("u"=>"bibo:Book"),
                            array("u"=>"acorn:Work")
                        ),
                        "acorn:resourceCount"=>array(
                            "l"=>"0"
                        ),
                        "acorn:isbnCount"=>array(
                            "l"=>"1"
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/4d101f63c10a6","c"=>"http://talisaspire.com/"),
                        "rdf:type"=>array(
                            array("u"=>"bibo:Book"),
                            array("u"=>"acorn:Work")
                        ),
                        "acorn:seeAlso"=>array(
                            "u"=>"http://talisaspire.com/works/4d101f63c10a6-2"
                        ),
                        "acorn:resourceCount"=>array(
                            "l"=>"2"
                        ),
                        "acorn:resourceCountAlt"=>array(
                            "l"=>"0"
                        ),
                        "acorn:isbnCount"=>array(
                            "l"=>"2"
                        )
                    )
                ),
                _EXPIRES=>$expiryDate
            )
        );

        $actualView = \Tripod\Mongo\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_counts')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/works/4d101f63c10a6',"c"=>"http://talisaspire.com/","type"=>'v_counts')));
//        var_dump($actualView); die;
//        var_dump($expectedView);
        $this->assertEquals($expectedView,$actualView);
    }

    public function testGetViewWithNamespaces()
    {
        $g = $this->tripodViews->getViewForResource("baseData:1","v_work_see_also","baseData:DefaultGraph");
        $this->assertFalse($g->is_empty(),"Graph should not be empty");
        $this->assertTrue($g->get_subject_subgraph('http://talisaspire.com/works/4d101f63c10a6-2')->is_empty(),"Graph for see also should be empty, as does not exist in requested context");

        $g2 = $this->tripodViews->getViewForResource("baseData:2","v_work_see_also","baseData:DefaultGraph");
        $this->assertFalse($g2->is_empty(),"Graph should not be empty");
        $this->assertFalse($g2->get_subject_subgraph('http://basedata.com/b/2')->is_empty(),"Graph for see also should be populated, as does exist in requested context");

        // use a mock heron-in to make sure generateView is not called again for different combinations of qname/full uri

        /* @var $mockTripodViews \Tripod\Mongo\Views */
        $mockTripodViews = $this->getMock('Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews->expects($this->never())->method('generateView');

        $g3 = $mockTripodViews->getViewForResource("http://basedata.com/b/2","v_work_see_also","http://basedata.com/b/DefaultGraph");
        $g4 = $mockTripodViews->getViewForResource("baseData:2","v_work_see_also","http://basedata.com/b/DefaultGraph");
        $g5 = $mockTripodViews->getViewForResource("http://basedata.com/b/2","v_work_see_also","baseData:DefaultGraph");
        $this->assertEquals($g2->to_ntriples(),$g3->to_ntriples(),"View requested with subject/context qnamed should be equal to that with unnamespaced params");
        $this->assertEquals($g2->to_ntriples(),$g4->to_ntriples(),"View requested with subject/context qnamed should be equal to that with only resource namespaced");
        $this->assertEquals($g2->to_ntriples(),$g5->to_ntriples(),"View requested with subject/context qnamed should be equal to that with only context namespaced");
    }

    public function testGenerateViewsForResourcesOfTypeWithNamespace()
    {
        /* @var $mockTripodViews \Tripod\Mongo\Views */
        $mockTripodViews = $this->getMock('Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(array("ok"=>true)));

        // spec is namespaced, acorn:Work, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType("http://talisaspire.com/schema#Work");

        /* @var $mockTripodViews \Tripod\Mongo\Views */
        $mockTripodViews = $this->getMock('Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(array("ok"=>true)));

        // spec is fully qualified, http://talisaspire.com/shema#Work2, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType("acorn:Work2");
    }

    // todo: more unit tests to cover other view spec/search document properties: condition, maxJoins, followSequence, from

    public function testGetViewForResourcesDoesNotInvokeViewGenerationForMissingResources()
    {
        $uri1 = "http://uri1";
        $uri2 = "http://uri2";

        $viewType = "someView";
        $context = "http://someContext";

        $query = array(
            "_id" => array(
                '$in' => array(
                    array("r"=>$uri1,"c"=>$context,"type"=>$viewType),
                    array("r"=>$uri2,"c"=>$context,"type"=>$viewType)
                )
            )
        );

        $returnedGraph = new ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1,'http://somepred','someval');

        $mockDb = $this->getMock("MongoDB", array("selectCollection"),array(new MongoClient(),"test"));
        $mockColl = $this->getMock("MongoCollection", array("findOne"),array($mockDb,$this->tripod->getPodName()));
        $mockViewColl = $this->getMock("MongoCollection", array("findOne"),array($mockDb,VIEWS_COLLECTION));

        $mockDb->expects($this->any())->method("selectCollection")->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method("findOne")->will($this->returnValue(null));


        /** @var PHPUnit_Framework_MockObject_MockObject|TripodTestConfig $mockConfig */
        $mockConfig = $this->getMock(
            'TripodTestConfig',
            array('getCollectionForCBD','getCollectionForView')
        );

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(\Tripod\Mongo\Config::getConfig());

        /* @var $mockTripodViews|PHPUnit_Framework_MockObject_MockObject Views */
        $mockTripodViews = $this->getMock(
            'Views',
            array('generateView','fetchGraph', 'getMongoTripodConfigInstance'),
            array('tripod_php_testing',$mockColl,$context)
        );

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->once())
            ->method("fetchGraph")
            ->with($query,MONGO_VIEW,$mockViewColl, null, 101)
            ->will($this->returnValue($returnedGraph));

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getMongoTripodConfigInstance')
            ->will($this->returnValue($mockConfig));

        $resultGraph = $mockTripodViews->getViewForResources(array($uri1,$uri2),$viewType,$context);

        $this->assertEquals($returnedGraph->to_ntriples(),$resultGraph->to_ntriples());
    }

    public function testGetViewForResourcesInvokesViewGenerationForMissingResources()
    {
        $uri1 = "http://uri1";
        $uri2 = "http://uri2";

        $viewType = "someView";
        $context = "http://someContext";

        $mockDb = $this->getMock("MongoDB", array("selectCollection"),array(new MongoClient(),"test"));
        $mockColl = $this->getMock("MongoCollection", array("findOne"),array($mockDb,$this->tripod->getPodName()));
        $mockViewColl = $this->getMock("MongoCollection", array("findOne"),array($mockDb,VIEWS_COLLECTION));

        $mockDb->expects($this->any())->method("selectCollection")->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method("findOne")->will($this->returnValue(array("_id"=>$uri1))); // the actual returned doc is not important, it just has to not be null


        /** @var PHPUnit_Framework_MockObject_MockObject|TripodTestConfig $mockConfig */
        $mockConfig = $this->getMock(
            'TripodTestConfig',
            array('getCollectionForCBD', 'getCollectionForView')
        );

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(\Tripod\Mongo\Config::getConfig());


        /* @var $mockTripodViews \Tripod\Mongo\Views */
        $mockTripodViews = $this->getMock(
            'Views',
            array('generateView','fetchGraph','getMongoTripodConfigInstance'),
            array('tripod_php_testing',$mockColl,$context)
        );

        $mockTripodViews->expects($this->once())
            ->method('generateView')
            ->with($viewType,$uri2,$context)
            ->will($this->returnValue(array("ok"=>true)));

        $mockTripodViews->expects($this->exactly(2))
            ->method("fetchGraph")
            ->will($this->returnCallback(array($this, 'fetchGraphInGetViewForResourcesCallback')));

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getMongoTripodConfigInstance')
            ->will($this->returnValue($mockConfig));

        $resultGraph = $mockTripodViews->getViewForResources(array($uri1,$uri2),$viewType,$context);

        $expectedGraph = new ExtendedGraph();
        $expectedGraph->add_literal_triple($uri1,'http://somepred','someval');
        $expectedGraph->add_literal_triple($uri2,'http://somepred','someval');

        $this->assertEquals($expectedGraph->to_ntriples(),$resultGraph->to_ntriples());
    }

    public function testDeletionOfResourceTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new MongoTripodLabeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new MongoTripod('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('_id.r'=>$labeller->uri_to_alias($uri1), '_id.c'=>$context)));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri1)=>array(
                'rdf:type','searchterms:topic','dct:isVersionOf'
            )
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'Tripod',
            array(
                'getDataUpdater', 'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            'Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock('Views',
            array('generateViewsForResourcesOfType'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                'http://talisaspire.com/'
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $context
            );

        $mockViews->expects($this->never())
            ->method('generateViewsForResourcesOfType');


        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri1), new ExtendedGraph());

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1),
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_full_ttl, because TTL views don't include impactIndex
                array('v_resource_full', 'v_resource_to_single_source')
            )
        );

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach($collections as $collection)
        {
            $this->assertEquals(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }
    }

    /**
     * Basically identical to testDeletionOfResourceTriggersViewRegeneration, but focus on $url2, instead
     */
    public function testDeletionOfResourceInImpactIndexTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new MongoTripodLabeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new MongoTripod('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri2)=>array(
                'rdf:type','dct:subject'
            )
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'Tripod',
            array(
                'getDataUpdater', 'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            'Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock('Views',
            array('generateView'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                'http://talisaspire.com/'
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $context
            );

        // Because we're not deleting $url1, the all the views for it will regenerate
        $mockViews->expects($this->exactly(3))
            ->method('generateView')
            ->withConsecutive(
                array(
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );


        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), new ExtendedGraph());

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                array('v_resource_full')
            )
        );

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach($collections as $collection)
        {
            $this->assertEquals(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }
    }

    /**
     * Basically identical to testDeletionOfResourceInImpactIndexTriggersViewRegeneration, but update $url2, rather
     * than deleting it
     */
    public function testUpdateOfResourceInImpactIndexTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new MongoTripodLabeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new MongoTripod('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri2)=>array('dct:subject')
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'Tripod',
            array(
                'getDataUpdater', 'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            'Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock('Views',
            array('generateView'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                'http://talisaspire.com/'
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $context
            );

        // Because we're not deleting $url1, the all the views for it will regenerate
        $mockViews->expects($this->exactly(3))
            ->method('generateView')
            ->withConsecutive(
                array(
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );


        $newGraph = $originalGraph->get_subject_subgraph($uri2);
        $newGraph->replace_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria', 'Grab bag');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), $newGraph);

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                array('v_resource_full')
            )
        );

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach($collections as $collection)
        {
            $this->assertEquals(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }
    }

    /**
     * Similar to testDeletionOfResourceTriggersViewRegeneration except $url1 is updated, rather than deleted
     */
    public function testUpdateOfResourceTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new MongoTripodLabeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new MongoTripod('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('_id.r'=>$labeller->uri_to_alias($uri1), '_id.c'=>$context)));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri1)=>array('dct:title')
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'Tripod',
            array(
                'getDataUpdater', 'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            'Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock('Views',
            array('generateView'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context
            )
        );

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                'http://talisaspire.com/'
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $context
            );

        // Because we're not deleting $url1, the all the views for it will regenerate
        $mockViews->expects($this->exactly(3))
            ->method('generateView')
            ->withConsecutive(
                array(
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:title'), 'Title of Resource');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri1), $newGraph);

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                array('v_resource_full', 'v_resource_to_single_source')
            )
        );

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach($impactedSubjects as $subject)
        {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach($collections as $collection)
        {
            $this->assertEquals(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }
    }

    /**
     * Test that a change to a resource that isn't covered by a viewspec or in an impact index still triggers the discover
     * impacted subjects operation and returns nothing
     */
    public function testResourceUpdateNotCoveredBySpecStillTriggersOperations()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new MongoTripodLabeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:title'), 'How to speak American like a native');
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:subject'), 'Languages -- \'Murrican');


        $originalSubjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri1)=>array('rdf:type','dct:title','dct:subject')
        );

        $updatedSubjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri1)=>array('dct:subject')
        );

        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            'Tripod',
            array(
                'getDataUpdater', 'getComposite'
            ),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockTripodUpdates = $this->getMock(
            'Updates',
            array(
                'processSyncOperations',
                'queueAsyncOperations'
            ),
            array(
                $mockTripod,
                array(
                    OP_ASYNC=>array(
                        OP_TABLES=>true,
                        OP_VIEWS=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $mockViews = $this->getMock('Views',
            array('generateViewsForResourcesOfType'),
            array(
                'tripod_php_testing',
                \Tripod\Mongo\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context
            )
        );

        $mockTripod->expects($this->exactly(2))
            ->method('getDataUpdater')
            ->will($this->returnValue($mockTripodUpdates));

        $mockTripod->expects($this->exactly(2))
            ->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($mockViews));

        $mockTripodUpdates->expects($this->exactly(2))
            ->method('processSyncOperations')
            ->withConsecutive(
                array(
                    $this->equalTo($originalSubjectsAndPredicatesOfChange),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo($updatedSubjectsAndPredicatesOfChange),
                    $this->equalTo($context)
                )
            );

        $mockTripodUpdates->expects($this->exactly(2))
            ->method('queueAsyncOperations')
            ->withConsecutive(
                array(
                    $this->equalTo($originalSubjectsAndPredicatesOfChange),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo($updatedSubjectsAndPredicatesOfChange),
                    $this->equalTo($context)
                )
            );

        $mockViews->expects($this->never())
            ->method('generateViewsForResourcesOfType');

        $mockTripod->saveChanges(new ExtendedGraph(), $originalGraph);

        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $impactedSubjects = $view->getImpactedSubjects($originalSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->replace_literal_triple($uri1, $labeller->qname_to_uri('dct:subject'), 'Languages -- \'Murrican', 'Languages -- English, American');

        $mockTripod->saveChanges($originalGraph, $newGraph);


        /** @var \Tripod\Mongo\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('Views', $view);

        $impactedSubjects = $view->getImpactedSubjects($updatedSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

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
        $g = new MongoGraph();
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

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("bibo:Journal"));  // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:title"),   "This is yet another new resource");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "art");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "design");
        $subjectsAndPredicatesOfChange = array(
            $newSubjectUri1=>array('rdf:type','dct:creator','dct:title','dct:subject'),
            $newSubjectUri2=>array('rdf:type','dct:creator','dct:title','dct:subject'),
            $newSubjectUri3=>array('rdf:type','dct:creator','dct:title','dct:subject')
        );
        $tripod->saveChanges(new MongoGraph(), $g);

        /** @var \Tripod\Mongo\Views $views */
        $views = $tripod->getComposite(OP_VIEWS);

        $expectedImpactedSubjects = array(
            new ImpactedSubject(
                array(
                    _ID_RESOURCE=>$newSubjectUri2,
                    _ID_CONTEXT=>'http://talisaspire.com/'
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                array()
            )
        );

        $impactedSubjects = $views->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    function fetchGraphInGetViewForResourcesCallback()
    {
        $uri1 = "http://uri1";
        $uri2 = "http://uri2";

        $viewType = "someView";
        $context = "http://someContext";

        $query1 = array("_id"=>array('$in'=>array(array("r"=>$uri1,"c"=>$context,"type"=>$viewType),array("r"=>$uri2,"c"=>$context,"type"=>$viewType))));
        $query2 = array("_id"=>array('$in'=>array(array("r"=>$uri2,"c"=>$context,"type"=>$viewType))));

        $returnedGraph1 = new ExtendedGraph();
        $returnedGraph1->add_literal_triple($uri1,'http://somepred','someval');

        $returnedGraph2 = new ExtendedGraph();
        $returnedGraph2->add_literal_triple($uri2,'http://somepred','someval');

        $args = func_get_args();
        if($args[0]==$query1){
            return $returnedGraph1;
        }
        else if($args[0]==$query2){
            return $returnedGraph2;
        }
        else
        {
            $this->fail();
        }
    }
}