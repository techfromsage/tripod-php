<?php

require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';
require_once 'src/mongo/delegates/MongoTripodViews.class.php';

class MongoTripodViewsTest extends MongoTripodTestBase {
    /**
     * @var MongoTripodViews
     */
    protected $tripodViews = null;

    private $viewsConstParams = null;
    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new MongoTransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        /** @var MongoTripod|PHPUnit_Framework_MockObject_MockObject $this->tripod */
        $this->tripod = $this->getMock(
            'MongoTripod',
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

        $this->tripodViews = new MongoTripodViews(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/'
        );


        foreach(MongoTripodConfig::getInstance()->getCollectionsForViews($this->tripod->getStoreName()) as $collection)
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
        $mongo = new MongoClient(MongoTripodConfig::getInstance()->getConnStr('tripod_php_testing'));
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',"c"=>'http://talisaspire.com/',"type"=>'v_resource_full')));
        $this->assertEquals($expectedView,$actualView);
    }

    public function testGenerateViewWithTTL()
    {
        $expiryDate = new MongoDate(time()+300);
        $mockTripodViews = $this->getMock('MongoTripodViews', array('getExpirySecFromNow'), $this->viewsConstParams);
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
        $mongo = new MongoClient(MongoTripodConfig::getInstance()->getConnStr('tripod_php_testing'));
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',"c"=>'http://talisaspire.com/',"type"=>'v_resource_full_ttl')));
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
        $mockTripodViews = $this->getMock('MongoTripodViews', array('getExpirySecFromNow'), $this->viewsConstParams);
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time()-300)));

        $mockTripodViews->generateView("v_resource_full_ttl","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA");

        // now mock out generate views and check it's called...
        $mockTripodViews2 = $this->getMock('MongoTripodViews', array('generateView'), $this->viewsConstParams);
        $mockTripodViews2->expects($this->once())->method('generateView')->with('v_resource_full_ttl','http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $mockTripodViews2->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','v_resource_full_ttl');
    }

    public function testGenerateViewWithCountAggregate()
    {
        $expiryDate = new MongoDate(time()+300);
        /**
         * @var $mockTripodViews MongoTripodViews
         */
        $mockTripodViews = $this->getMock('MongoTripodViews', array('getExpirySecFromNow'), $this->viewsConstParams);
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
        $mongo = new MongoClient(MongoTripodConfig::getInstance()->getConnStr('tripod_php_testing'));
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/works/4d101f63c10a6',"c"=>"http://talisaspire.com/","type"=>'v_counts')));
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

        /* @var $mockTripodViews MongoTripodViews */
        $mockTripodViews = $this->getMock('MongoTripodViews', array('generateView'), $this->viewsConstParams);
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
        /* @var $mockTripodViews MongoTripodViews */
        $mockTripodViews = $this->getMock('MongoTripodViews', array('generateView'), $this->viewsConstParams);
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(array("ok"=>true)));

        // spec is namespaced, acorn:Work, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType("http://talisaspire.com/schema#Work");

        /* @var $mockTripodViews MongoTripodViews */
        $mockTripodViews = $this->getMock('MongoTripodViews', array('generateView'), $this->viewsConstParams);
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

        $mockDb->expects($this->any())->method("selectCollection")->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method("findOne")->will($this->returnValue(null));

        /** @var PHPUnit_Framework_MockObject_MockObject|MongoTripodTestConfig $mockConfig */
        $mockConfig = $this->getMock(
            'MongoTripodTestConfig',
            array('getCollectionForCBD')
        );

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->loadConfig(MongoTripodConfig::getConfig());

        /* @var $mockTripodViews|PHPUnit_Framework_MockObject_MockObject MongoTripodViews */
        $mockTripodViews = $this->getMock(
            'MongoTripodViews',
            array('generateView','fetchGraph', 'getMongoTripodConfigInstance'),
            array('tripod_php_testing',$mockColl,$context)
        );
        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->once())
            ->method("fetchGraph")
            ->with($query,MONGO_VIEW,VIEWS_COLLECTION, null, 101)
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

        $mockDb->expects($this->any())->method("selectCollection")->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method("findOne")->will($this->returnValue(array("_id"=>$uri1))); // the actual returned doc is not important, it just has to not be null


        /** @var PHPUnit_Framework_MockObject_MockObject|MongoTripodTestConfig $mockConfig */
        $mockConfig = $this->getMock(
            'MongoTripodTestConfig',
            array('getCollectionForCBD')
        );

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->loadConfig(MongoTripodConfig::getConfig());


        /* @var $mockTripodViews MongoTripodViews */
        $mockTripodViews = $this->getMock(
            'MongoTripodViews',
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