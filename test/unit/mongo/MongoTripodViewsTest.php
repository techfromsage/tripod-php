<?php

require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/Driver.class.php';
require_once 'src/mongo/delegates/Views.class.php';

use \Tripod\Mongo\Composites\Views;
use \MongoDB\BSON\UTCDateTime;
use \MongoDB\Client;

class MongoTripodViewsTest extends MongoTripodTestBase {
    /**
     * @var \Tripod\Mongo\Composites\Views
     */
    protected $tripodViews = null;

    private $viewsConstParams = null;
    protected function setUp()
    {
        parent::setup();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $this->tripod */
        $this->tripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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

        $this->tripodViews = new \Tripod\Mongo\Composites\Views(
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
        $this->loadResourceDataViaTripod();
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
        $mongo = new Client(
            \Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'),
            [],
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',"c"=>'http://talisaspire.com/',"type"=>'v_resource_full')));
        $this->assertEquals($expectedView,$actualView);
    }

    /**
     * Tests view filters removes data, but keeps it in the impact index
     */
    public function testGenerateViewWithFilterRemovesFilteredDataButKeepsResourcesInTheImpactIndex()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource("http://talisaspire.com/resources/filter1","v_resource_filter1");

        $expectedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/filter1",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_filter1"),
            "value"=>array(
                _GRAPHS=>array(
                    // This Book should not be included in the view - we are filtering to include only chapters.
                    //
                    // array(
                    //     "_id"=>array("r"=>"http://talisaspire.com/works/filter1","c"=>'http://talisaspire.com/'),
                    //     "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                    //     "rdf:type"=>array(
                    //         array(VALUE_URI=>"bibo:Book"),
                    //         array(VALUE_URI=>"acorn:Work")
                    //     )
                    // ),
                     array(
                         "_id"=>array("r"=>"http://talisaspire.com/works/filter3","c"=>'http://talisaspire.com/'),
                         "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                         "rdf:type"=>array(
                             array(VALUE_URI=>"bibo:Chapter"),
                             array(VALUE_URI=>"acorn:Work")
                         )
                     ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/filter1","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource"),
                            array(VALUE_URI=>"rdf:Seq")
                        ),
                        "searchterms:topic"=>array(
                            array(VALUE_LITERAL=>"engineering: general"),
                            array(VALUE_LITERAL=>"physics"),
                            array(VALUE_LITERAL=>"science")
                        ),
                        "dct:isVersionOf"=>array(
                            array(VALUE_URI=>"http://talisaspire.com/works/filter1"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter2"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter3")
                        )
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    // This item has been filtered - but it should still be in the impact index
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter2",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter3",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );
        // get the view direct from mongo
        $mongo = new Client(
            \Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'),
            [],
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/filter1',"c"=>'http://talisaspire.com/',"type"=>'v_resource_filter1')));
        $this->assertEquals($expectedView,$actualView);
    }

    /**
     * Tests view filter by literal values
     */
    public function testGenerateViewWithFilterOnLiteralValue()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource("http://talisaspire.com/resources/filter1","v_resource_filter2");

        $expectedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/filter1",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_filter2"),
            "value"=>array(
                _GRAPHS=>array(
                    // http://talisaspire.com/works/filter2 has the matching literal
                    // http://talisaspire.com/works/filter1 doesn't - it should not be in the results
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter2","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/filter1","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource"),
                            array(VALUE_URI=>"rdf:Seq")
                        ),
                        "searchterms:topic"=>array(
                            array(VALUE_LITERAL=>"engineering: general"),
                            array(VALUE_LITERAL=>"physics"),
                            array(VALUE_LITERAL=>"science")
                        ),
                        "dct:isVersionOf"=>array(
                            array(VALUE_URI=>"http://talisaspire.com/works/filter1"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter2"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter3")
                        )
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    // This item has been filtered - but it should still be in the impact index
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter2",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter3",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );
        // get the view direct from mongo
        $mongo = new Client(
            \Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'),
            [],
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(
            array('_id'=>array("r"=>'http://talisaspire.com/resources/filter1',"c"=>'http://talisaspire.com/',"type"=>'v_resource_filter2'))
        );
        $this->assertEquals($expectedView,$actualView);
    }

    /**
     * Test data removed from view by filter is included in view after update and regeneration
     */
    public function testGenerateViewCorrectlyAfterUpdateAffectsFilter()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource("http://talisaspire.com/resources/filter1","v_resource_filter1");

        $expectedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/filter1",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_filter1"),
            "value"=>array(
                _GRAPHS=>array(
                    // This Book should not be included in the view - we are filtering to include only chapters.
                    //
                    // array(
                    //     "_id"=>array("r"=>"http://talisaspire.com/works/filter1","c"=>'http://talisaspire.com/'),
                    //     "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                    //     "rdf:type"=>array(
                    //         array(VALUE_URI=>"bibo:Book"),
                    //         array(VALUE_URI=>"acorn:Work")
                    //     )
                    // ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter3","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Chapter"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/filter1","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource"),
                            array(VALUE_URI=>"rdf:Seq")
                        ),
                        "searchterms:topic"=>array(
                            array(VALUE_LITERAL=>"engineering: general"),
                            array(VALUE_LITERAL=>"physics"),
                            array(VALUE_LITERAL=>"science")
                        ),
                        "dct:isVersionOf"=>array(
                            array(VALUE_URI=>"http://talisaspire.com/works/filter1"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter2"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter3")
                        )
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    // This item has been filtered - but it should still be in the impact index
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter2",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter3",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );
        // get the view direct from mongo
        $mongo = new Client(
            \Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'),
            [],
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/filter1',"c"=>'http://talisaspire.com/',"type"=>'v_resource_filter1')));
        $this->assertEquals($expectedView,$actualView);

        // Modify http://talisaspire.com/works/filter1 so that it is a Chapter (included in the view) not a Book (excluded from the view)
        $oldGraph = new \Tripod\ExtendedGraph();
        $oldGraph->add_resource_triple("http://talisaspire.com/works/filter1","http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://purl.org/ontology/bibo/Book");
        $newGraph = new \Tripod\ExtendedGraph();
        $newGraph->add_resource_triple("http://talisaspire.com/works/filter1","http://www.w3.org/1999/02/22-rdf-syntax-ns#type","http://purl.org/ontology/bibo/Chapter");

        $context = 'http://talisaspire.com/';
        $tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges($oldGraph,$newGraph);


        $expectedUpdatedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/filter1",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_filter1"),
            "value"=>array(
                _GRAPHS=>array(
                    // This work is now included as it's type has changed to Chapter
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter1","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"acorn:Work"),
                            array(VALUE_URI=>"bibo:Chapter")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter3","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Chapter"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/filter1","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource"),
                            array(VALUE_URI=>"rdf:Seq")
                        ),
                        "searchterms:topic"=>array(
                            array(VALUE_LITERAL=>"engineering: general"),
                            array(VALUE_LITERAL=>"physics"),
                            array(VALUE_LITERAL=>"science")
                        ),
                        "dct:isVersionOf"=>array(
                            array(VALUE_URI=>"http://talisaspire.com/works/filter1"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter2"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter3")
                        )
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    // This item has been filtered - but it should still be in the impact index
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter2",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter3",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );

        $updatedView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/filter1',"c"=>'http://talisaspire.com/',"type"=>'v_resource_filter1')));
        $this->assertEquals($expectedUpdatedView,$updatedView);
    }

    /**
     * Test including an rdf sequence in a view
     */
    public function testGenerateViewContainingRdfSequence()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource("http://talisaspire.com/resources/filter1","v_resource_rdfsequence");

        $expectedView = array(
            "_id"=>array(
                _ID_RESOURCE=>"http://talisaspire.com/resources/filter1",
                _ID_CONTEXT=>'http://talisaspire.com/',
                "type"=>"v_resource_rdfsequence"),
            "value"=>array(
                _GRAPHS=>array(
                     array(
                         "_id"=>array("r"=>"http://talisaspire.com/works/filter1","c"=>'http://talisaspire.com/'),
                         "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                         "rdf:type"=>array(
                             array(VALUE_URI=>"bibo:Book"),
                             array(VALUE_URI=>"acorn:Work")
                         )
                     ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter2","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/works/filter3","c"=>'http://talisaspire.com/'),
                        "dct:subject"=>array("u"=>"http://talisaspire.com/disciplines/physics"),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Chapter"),
                            array(VALUE_URI=>"acorn:Work")
                        )
                    ),
                    array(
                        "_id"=>array("r"=>"http://talisaspire.com/resources/filter1","c"=>'http://talisaspire.com/'),
                        "rdf:type"=>array(
                            array(VALUE_URI=>"bibo:Book"),
                            array(VALUE_URI=>"acorn:Resource"),
                            array(VALUE_URI=>"rdf:Seq")
                        ),
                        "dct:isVersionOf"=>array(
                            array(VALUE_URI=>"http://talisaspire.com/works/filter1"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter2"),
                            array(VALUE_URI=>"http://talisaspire.com/works/filter3")
                        ),
                        "rdf:_1"=>array("u"=>"http://talisaspire.com/1"),
                        "rdf:_2"=>array("u"=>"http://talisaspire.com/2"),
                        "rdf:_3"=>array("u"=>"http://talisaspire.com/3")
                    )
                ),
                _IMPACT_INDEX=>array(
                    array(_ID_RESOURCE=>"http://talisaspire.com/resources/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    // This item has been filtered - but it should still be in the impact index
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter1",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter2",_ID_CONTEXT=>'http://talisaspire.com/'),
                    array(_ID_RESOURCE=>"http://talisaspire.com/works/filter3",_ID_CONTEXT=>'http://talisaspire.com/')
                )
            )
        );
        // get the view direct from mongo
        $mongo = new Client(
            \Tripod\Mongo\Config::getInstance()->getConnStr('tripod_php_testing'),
            [],
            ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]
        );
        $actualView = $mongo->selectCollection('tripod_php_testing','views')->findOne(array('_id'=>array("r"=>'http://talisaspire.com/resources/filter1',"c"=>'http://talisaspire.com/',"type"=>'v_resource_rdfsequence')));

        $this->assertEquals($expectedView,$actualView);
    }

    public function testGenerateViewWithTTL()
    {
        $expiryDate = new UTCDateTime((time()+300)*1000);
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('getExpirySecFromNow'), $this->viewsConstParams);
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
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('getExpirySecFromNow'), $this->viewsConstParams);
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time()-300)));

        $mockTripodViews->generateView("v_resource_full_ttl","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA");

        // now mock out generate views and check it's called...
        $mockTripodViews2 = $this->getMock('\Tripod\Mongo\Composites\Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews2->expects($this->once())->method('generateView')->with('v_resource_full_ttl','http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $mockTripodViews2->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA','v_resource_full_ttl');
    }

    public function testGenerateViewWithCountAggregate()
    {
        $expiryDate = new UTCDateTime((time()+300)*1000);
        /**
         * @var $mockTripodViews \Tripod\Mongo\Composites\Views
         */
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('getExpirySecFromNow'), $this->viewsConstParams);
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

        /* @var $mockTripodViews \Tripod\Mongo\Composites\Views */
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('generateView'), $this->viewsConstParams);
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
        /* @var $mockTripodViews \Tripod\Mongo\Composites\Views */
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('generateView'), $this->viewsConstParams);
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(array("ok"=>true)));

        // spec is namespaced, acorn:Work, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType("http://talisaspire.com/schema#Work");

        /* @var $mockTripodViews \Tripod\Mongo\Composites\Views */
        $mockTripodViews = $this->getMock('\Tripod\Mongo\Composites\Views', array('generateView'), $this->viewsConstParams);
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

        $returnedGraph = new \Tripod\ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1,'http://somepred','someval');

        $mockDb = $this->getMockBuilder('\MongoDB\Database')
            ->disableOriginalConstructor()
            ->setMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();

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
            '\Tripod\Mongo\Composites\Views',
            array('generateView','fetchGraph', 'getConfigInstance'),
            array('tripod_php_testing',$mockColl,$context)
        );

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->once())
            ->method("fetchGraph")
            ->with($query,MONGO_VIEW,$mockViewColl, null, 101)
            ->will($this->returnValue($returnedGraph));

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
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

        $mockDb = $this->getMockBuilder('\MongoDB\Database')
            ->disableOriginalConstructor()
            ->setMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();

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


        /* @var $mockTripodViews \Tripod\Mongo\Composites\Views */
        $mockTripodViews = $this->getMock(
            '\Tripod\Mongo\Composites\Views',
            array('generateView','fetchGraph','getConfigInstance'),
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
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $resultGraph = $mockTripodViews->getViewForResources(array($uri1,$uri2),$viewType,$context);

        $expectedGraph = new \Tripod\ExtendedGraph();
        $expectedGraph->add_literal_triple($uri1,'http://somepred','someval');
        $expectedGraph->add_literal_triple($uri2,'http://somepred','someval');

        $this->assertEquals($expectedGraph->to_ntriples(),$resultGraph->to_ntriples());
    }

    public function testDeletionOfResourceTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new \Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new \Tripod\ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph);

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

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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
            '\Tripod\Mongo\Updates',
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

        $mockViews = $this->getMock('\Tripod\Mongo\Composites\Views',
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


        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri1), new \Tripod\ExtendedGraph());

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1),
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_full_ttl, because TTL views don't include impactIndex
                array('v_resource_full', 'v_resource_to_single_source', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence')
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

        $labeller = new \Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new \Tripod\ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph);

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

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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
            '\Tripod\Mongo\Updates',
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

        $mockViews = $this->getMock('\Tripod\Mongo\Composites\Views',
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
        $mockViews->expects($this->exactly(6))
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
                ),
                array(
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );


        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), new \Tripod\ExtendedGraph());

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                array('v_resource_full','v_resource_filter1','v_resource_filter2', 'v_resource_rdfsequence')
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

        $labeller = new \Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new \Tripod\ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri1), 'c'=>$context))));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri2)=>array('dct:subject')
        );

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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
            '\Tripod\Mongo\Updates',
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

        $mockViews = $this->getMock('\Tripod\Mongo\Composites\Views',
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
        $mockViews->expects($this->exactly(6))
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
                ),
                array(
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );


        $newGraph = $originalGraph->get_subject_subgraph($uri2);
        $newGraph->replace_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria', 'Grab bag');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), $newGraph);

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                array('v_resource_full', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence')
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

        $labeller = new \Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new \Tripod\ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', array('defaultContext'=>$context));
        $tripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph);

        $collections = \Tripod\Mongo\Config::getInstance()->getCollectionsForViews('tripod_php_testing', array('v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source'));

        foreach($collections as $collection)
        {
            $this->assertGreaterThan(0, $collection->count(array('_id.r'=>$labeller->uri_to_alias($uri1), '_id.c'=>$context)));
        }

        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri1)=>array('dct:title')
        );

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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
            '\Tripod\Mongo\Updates',
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

        $mockViews = $this->getMock('\Tripod\Mongo\Composites\Views',
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
        $mockViews->expects($this->exactly(6))
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
                ),
                array(
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context)
                )
            );

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:title'), 'Title of Resource');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri1), $newGraph);

        // Walk through the processSyncOperations process manually for views

        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
                array(
                    _ID_RESOURCE=>$labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT=>$context
                ),
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                array('v_resource_full', 'v_resource_to_single_source', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence')
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

        $labeller = new \Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new \Tripod\ExtendedGraph();

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

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $mockTripod */
        $mockTripod = $this->getMock(
            '\Tripod\Mongo\Driver',
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
            '\Tripod\Mongo\Updates',
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

        $mockViews = $this->getMock('\Tripod\Mongo\Composites\Views',
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

        $mockTripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph);

        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $impactedSubjects = $view->getImpactedSubjects($originalSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->replace_literal_triple($uri1, $labeller->qname_to_uri('dct:subject'), 'Languages -- \'Murrican', 'Languages -- English, American');

        $mockTripod->saveChanges($originalGraph, $newGraph);


        /** @var \Tripod\Mongo\Composites\Views $view */
        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf('\Tripod\Mongo\Composites\Views', $view);

        $impactedSubjects = $view->getImpactedSubjects($updatedSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject()
    {
        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
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

        $tripodUpdates = $this->getMockBuilder('\Tripod\Mongo\Updates')
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
        $tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g);

        /** @var \Tripod\Mongo\Composites\Views $views */
        $views = $tripod->getComposite(OP_VIEWS);

        $expectedImpactedSubjects = array(
            new \Tripod\Mongo\ImpactedSubject(
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

    public function testSavingToAPreviouslyEmptySeqeunceUpdatesView()
    {
        // create a tripod with views sync
        $tripod = new \Tripod\Mongo\Driver("CBD_testing","tripod_php_testing",array(
            "defaultContext"=>"http://talisaspire.com/",
            "async"=>array(OP_VIEWS=>false)
        ));

        // should be no triples with "http://basedata.com/b/sequence123" as subject in existing view
        $view = $tripod->getViewForResource("http://basedata.com/b/docWithEmptySeq123","v_doc_with_seqeunce");
        $this->assertTrue($view->has_triples_about("http://basedata.com/b/docWithEmptySeq123"));
        $this->assertFalse($view->has_triples_about("http://basedata.com/b/sequence123"));

        $newGraph = new \Tripod\ExtendedGraph();
        $newGraph->add_resource_to_sequence("http://basedata.com/b/sequence123","http://basedata.com/b/sequenceItem123");

        $tripod->saveChanges(new \Tripod\ExtendedGraph(),$newGraph);

        // should be triples with "http://basedata.com/b/sequence123" as subject in new view
        $view = $this->tripod->getViewForResource("http://basedata.com/b/docWithEmptySeq123","v_doc_with_seqeunce");
        $this->assertTrue($view->has_triples_about("http://basedata.com/b/docWithEmptySeq123"));
        $this->assertTrue($view->has_triples_about("http://basedata.com/b/sequence123"));
    }

    public function testSavingToAPreviouslyEmptyJoinUpdatesView()
    {
        // create a tripod with views sync
        $tripod = new \Tripod\Mongo\Driver("CBD_testing","tripod_php_testing",array(
            "defaultContext"=>"http://talisaspire.com/",
            "async"=>array(OP_VIEWS=>false)
        ));

        // should be no triples with "http://basedata.com/b/sequence123" as subject in existing view
        $view = $tripod->getViewForResource("http://basedata.com/b/docWithEmptySeq123","v_doc_with_seqeunce");
        $this->assertTrue($view->has_triples_about("http://basedata.com/b/docWithEmptySeq123"));
        $this->assertFalse($view->has_triples_about("http://schemas.talis.com/2005/user/schema#xyz"));

        $newGraph = new \Tripod\ExtendedGraph();
        $newGraph->add_literal_triple("http://schemas.talis.com/2005/user/schema#xyz","http://rdfs.org/sioc/spec/name","Some name");

        $tripod->saveChanges(new \Tripod\ExtendedGraph(),$newGraph);

        // should be triples with "http://basedata.com/b/sequence123" as subject in new view
        $view = $tripod->getViewForResource("http://basedata.com/b/docWithEmptySeq123","v_doc_with_seqeunce");
        $this->assertTrue($view->has_triples_about("http://basedata.com/b/docWithEmptySeq123"));
        $this->assertTrue($view->has_triples_about("http://schemas.talis.com/2005/user/schema#xyz"));
    }

    /**
     * @return \Tripod\ExtendedGraph
     */
    public function fetchGraphInGetViewForResourcesCallback()
    {
        $uri1 = "http://uri1";
        $uri2 = "http://uri2";

        $viewType = "someView";
        $context = "http://someContext";

        $query1 = array("_id"=>array('$in'=>array(array("r"=>$uri1,"c"=>$context,"type"=>$viewType),array("r"=>$uri2,"c"=>$context,"type"=>$viewType))));
        $query2 = array("_id"=>array('$in'=>array(array("r"=>$uri2,"c"=>$context,"type"=>$viewType))));

        $returnedGraph1 = new \Tripod\ExtendedGraph();
        $returnedGraph1->add_literal_triple($uri1,'http://somepred','someval');

        $returnedGraph2 = new \Tripod\ExtendedGraph();
        $returnedGraph2->add_literal_triple($uri2,'http://somepred','someval');

        $args = func_get_args();
        if($args[0]==$query1){
            return $returnedGraph1;
        }
        elseif($args[0]==$query2)
        {
            return $returnedGraph2;
        }
        else
        {
            $this->fail();
        }
    }
    public function testCursorNoExceptions()
    {
        $uri1 = "http://uri1";

        $viewType = "someView";
        $context = "http://someContext";

        $returnedGraph = new \Tripod\ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1,'http://somepred','someval');

        $mockDb = $this->getMockBuilder('\MongoDB\Database')
            ->disableOriginalConstructor()
            ->setMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['find'])
            ->getMock();
        $mockCursor = $this->getMock('\ArrayIterator', array('rewind'));

        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

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
            '\Tripod\Mongo\Composites\Views',
            array('generateView', 'getConfigInstance'),
            array('tripod_php_testing',$mockColl,$context));

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $mockTripodViews->getViewForResources(array($uri1),$viewType,$context);
    }
    public function testCursorExceptionThrown()
    {
        $uri1 = "http://uri1";

        $viewType = "someView";
        $context = "http://someContext";

        $returnedGraph = new \Tripod\ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1,'http://somepred','someval');

        $mockDb = $this->getMockBuilder('\MongoDB\Database')
            ->disableOriginalConstructor()
            ->setMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne', 'find'])
            ->getMock();
        $mockCursor = $this->getMock('\ArrayIterator', array('rewind'));

        $mockCursor->expects($this->exactly(30))->method('rewind')->will($this->throwException(new \Exception('Exception thrown when cursoring to Mongo')));
        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

        $mockDb->expects($this->any())->method("selectCollection")->will($this->returnValue($mockColl));
        $mockColl->expects($this->never())->method("findOne");

        /** @var PHPUnit_Framework_MockObject_MockObject|TripodTestConfig $mockConfig */
        $mockConfig = $this->getMock(
            'TripodTestConfig',
            array('getCollectionForCBD','getCollectionForView')
        );

        $mockConfig->expects($this->never())
            ->method('getCollectionForCBD');

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(\Tripod\Mongo\Config::getConfig());

        /* @var $mockTripodViews|PHPUnit_Framework_MockObject_MockObject Views */
        $mockTripodViews = $this->getMock(
            '\Tripod\Mongo\Composites\Views',
            array('generateView', 'getConfigInstance'),
            array('tripod_php_testing',$mockColl,$context));

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $this->setExpectedException('\Exception', "Exception thrown when cursoring to Mongo");
        $mockTripodViews->getViewForResources(array($uri1),$viewType,$context);
    }
    public function testCursorNoExceptionThrownWhenCursorThrowsSomeExceptions()
    {
        $uri1 = "http://uri1";

        $viewType = "someView";
        $context = "http://someContext";

        $returnedGraph = new \Tripod\ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1,'http://somepred','someval');


        $mockDb = $this->getMockBuilder('\MongoDB\Database')
            ->disableOriginalConstructor()
            ->setMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder('\MongoDB\Collection')
            ->disableOriginalConstructor()
            ->setMethods(['find', 'findOne'])
            ->getMock();
        $mockCursor = $this->getMock('\ArrayIterator', array('rewind'));

        $mockCursor->expects($this->exactly(5))
            ->method('rewind')
            ->will($this->onConsecutiveCalls(
                $this->throwException(new \Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new \Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new \Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new \Exception('Exception thrown when cursoring to Mongo')),
                $this->returnValue($mockCursor)));

        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

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
            '\Tripod\Mongo\Composites\Views',
            array('generateView', 'getConfigInstance'),
            array('tripod_php_testing',$mockColl,$context));

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $mockTripodViews->getViewForResources(array($uri1),$viewType,$context);
    }
}