<?php

use PHPUnit\Framework\MockObject\MockObject;
use Tripod\ExtendedGraph;

class MongoTripodViewsTest extends MongoTripodTestBase
{
    /**
     * @var MockObject&Tripod\Mongo\Driver
     */
    protected $tripod;

    /**
     * @var Tripod\Mongo\Composites\Views
     */
    protected $tripodViews;

    private $viewsConstParams;

    protected function setUp(): void
    {
        parent::setup();

        $this->tripodTransactionLog = new Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => 'http://talisaspire.com/',
                    'async' => [OP_VIEWS => true], // don't generate views syncronously when saving automatically - let unit tests deal with this)
                ],
            ])
            ->getMock();

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->tripodViews = new Tripod\Mongo\Composites\Views(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/'
        );

        foreach (Tripod\Config::getInstance()->getCollectionsForViews($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }

        $this->viewsConstParams = [$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'];

        // load base data
        $this->loadResourceDataViaTripod();
    }

    /**
     * Tests view spec properties include + join
     */
    public function testGenerateView()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'v_resource_full');

        $expectedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_full'],
            'value' => [
                _GRAPHS => [
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_LITERAL => 'Testing'],
                        ],
                        'searchterms:topic' => [
                            [VALUE_LITERAL => 'engineering: general'],
                            [VALUE_LITERAL => 'physics'],
                            [VALUE_LITERAL => 'science'],
                        ],
                        'dct:isVersionOf' => [VALUE_URI => 'http://talisaspire.com/works/4d101f63c10a6'],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/4d101f63c10a6', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];
        // get the view direct from mongo
        $collection = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_full');
        $actualView = $collection->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_full']]);
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    /**
     * Tests view filters removes data, but keeps it in the impact index
     */
    public function testGenerateViewWithFilterRemovesFilteredDataButKeepsResourcesInTheImpactIndex()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource('http://talisaspire.com/resources/filter1', 'v_resource_filter1');

        $expectedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/filter1',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_filter1'],
            'value' => [
                _GRAPHS => [
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
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter3', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Chapter'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_URI => 'rdf:Seq'],
                        ],
                        'searchterms:topic' => [
                            [VALUE_LITERAL => 'engineering: general'],
                            [VALUE_LITERAL => 'physics'],
                            [VALUE_LITERAL => 'science'],
                        ],
                        'dct:isVersionOf' => [
                            [VALUE_URI => 'http://talisaspire.com/works/filter1'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter2'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter3'],
                        ],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    // This item has been filtered - but it should still be in the impact index
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter2', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter3', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];
        // get the view direct from mongo
        $collection = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_filter1');
        $actualView = $collection->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_filter1']]);
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    /**
     * Tests view filter by literal values
     */
    public function testGenerateViewWithFilterOnLiteralValue()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource('http://talisaspire.com/resources/filter1', 'v_resource_filter2');

        $expectedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/filter1',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_filter2'],
            'value' => [
                _GRAPHS => [
                    // http://talisaspire.com/works/filter2 has the matching literal
                    // http://talisaspire.com/works/filter1 doesn't - it should not be in the results
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter2', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_URI => 'rdf:Seq'],
                        ],
                        'searchterms:topic' => [
                            [VALUE_LITERAL => 'engineering: general'],
                            [VALUE_LITERAL => 'physics'],
                            [VALUE_LITERAL => 'science'],
                        ],
                        'dct:isVersionOf' => [
                            [VALUE_URI => 'http://talisaspire.com/works/filter1'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter2'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter3'],
                        ],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    // This item has been filtered - but it should still be in the impact index
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter2', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter3', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];
        // get the view direct from mongo
        $collection = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_filter2');
        $actualView = $collection->findOne(
            ['_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_filter2']]
        );
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    /**
     * Test data removed from view by filter is included in view after update and regeneration
     */
    public function testGenerateViewCorrectlyAfterUpdateAffectsFilter()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource('http://talisaspire.com/resources/filter1', 'v_resource_filter1');

        $expectedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/filter1',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_filter1'],
            'value' => [
                _GRAPHS => [
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
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter3', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Chapter'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_URI => 'rdf:Seq'],
                        ],
                        'searchterms:topic' => [
                            [VALUE_LITERAL => 'engineering: general'],
                            [VALUE_LITERAL => 'physics'],
                            [VALUE_LITERAL => 'science'],
                        ],
                        'dct:isVersionOf' => [
                            [VALUE_URI => 'http://talisaspire.com/works/filter1'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter2'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter3'],
                        ],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    // This item has been filtered - but it should still be in the impact index
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter2', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter3', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];
        // get the view direct from mongo
        $collection = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_filter1');
        $actualView = $collection->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_filter1']]);
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);

        // Modify http://talisaspire.com/works/filter1 so that it is a Chapter (included in the view) not a Book (excluded from the view)
        $oldGraph = new ExtendedGraph();
        $oldGraph->add_resource_triple('http://talisaspire.com/works/filter1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/ontology/bibo/Book');
        $newGraph = new ExtendedGraph();
        $newGraph->add_resource_triple('http://talisaspire.com/works/filter1', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/ontology/bibo/Chapter');

        $context = 'http://talisaspire.com/';
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => $context]);
        $tripod->saveChanges($oldGraph, $newGraph);

        $expectedUpdatedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/filter1',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_filter1'],
            'value' => [
                _GRAPHS => [
                    // This work is now included as it's type has changed to Chapter
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter1', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'acorn:Work'],
                            [VALUE_URI => 'bibo:Chapter'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter3', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Chapter'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_URI => 'rdf:Seq'],
                        ],
                        'searchterms:topic' => [
                            [VALUE_LITERAL => 'engineering: general'],
                            [VALUE_LITERAL => 'physics'],
                            [VALUE_LITERAL => 'science'],
                        ],
                        'dct:isVersionOf' => [
                            [VALUE_URI => 'http://talisaspire.com/works/filter1'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter2'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter3'],
                        ],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    // This item has been filtered - but it should still be in the impact index
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter2', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter3', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];

        $updatedView = $collection->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_filter1']]);
        $this->assertEquals($expectedUpdatedView['_id'], $updatedView['_id']);
        $this->assertEquals($expectedUpdatedView['value'], $updatedView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $updatedView['_cts']);
    }

    /**
     * Test including an rdf sequence in a view
     */
    public function testGenerateViewContainingRdfSequence()
    {
        // get the view - this should trigger generation
        $this->tripodViews->getViewForResource('http://talisaspire.com/resources/filter1', 'v_resource_rdfsequence');

        $expectedView = [
            '_id' => [
                _ID_RESOURCE => 'http://talisaspire.com/resources/filter1',
                _ID_CONTEXT => 'http://talisaspire.com/',
                'type' => 'v_resource_rdfsequence'],
            'value' => [
                _GRAPHS => [
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter1', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter2', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/filter3', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Chapter'],
                            [VALUE_URI => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            [VALUE_URI => 'bibo:Book'],
                            [VALUE_URI => 'acorn:Resource'],
                            [VALUE_URI => 'rdf:Seq'],
                        ],
                        'dct:isVersionOf' => [
                            [VALUE_URI => 'http://talisaspire.com/works/filter1'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter2'],
                            [VALUE_URI => 'http://talisaspire.com/works/filter3'],
                        ],
                        'rdf:_1' => ['u' => 'http://talisaspire.com/1'],
                        'rdf:_2' => ['u' => 'http://talisaspire.com/2'],
                        'rdf:_3' => ['u' => 'http://talisaspire.com/3'],
                    ],
                ],
                _IMPACT_INDEX => [
                    [_ID_RESOURCE => 'http://talisaspire.com/resources/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    // This item has been filtered - but it should still be in the impact index
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter1', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter2', _ID_CONTEXT => 'http://talisaspire.com/'],
                    [_ID_RESOURCE => 'http://talisaspire.com/works/filter3', _ID_CONTEXT => 'http://talisaspire.com/'],
                ],
            ],
        ];
        // get the view direct from mongo
        $collection = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_rdfsequence');
        $actualView = $collection->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/filter1', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_rdfsequence']]);

        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    public function testGenerateViewWithTTL()
    {
        $expiryDate = Tripod\Mongo\DateUtil::getMongoDate((time() + 300) * 1000);
        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getExpirySecFromNow'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time() + 300)));

        $mockTripodViews->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'v_resource_full_ttl');

        // should have expires, but no impact index
        $expectedView = [
            '_id' => [
                'r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                'c' => 'http://talisaspire.com/',
                'type' => 'v_resource_full_ttl'],
            'value' => [
                _GRAPHS => [
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6', 'c' => 'http://talisaspire.com/'],
                        'dct:subject' => ['u' => 'http://talisaspire.com/disciplines/physics'],
                        'rdf:type' => [
                            ['u' => 'bibo:Book'],
                            ['u' => 'acorn:Work'],
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            ['u' => 'bibo:Book'],
                            ['u' => 'acorn:Resource'],
                            ['l' => 'Testing'],
                        ],
                        'searchterms:topic' => [
                            ['l' => 'engineering: general'],
                            ['l' => 'physics'],
                            ['l' => 'science'],
                        ],
                        'dct:isVersionOf' => ['u' => 'http://talisaspire.com/works/4d101f63c10a6'],
                    ],
                ],
                _EXPIRES => $expiryDate,
            ],
        ];
        // get the view direct from mongo
        $actualView = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_resource_full_ttl')->findOne(['_id' => ['r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'c' => 'http://talisaspire.com/', 'type' => 'v_resource_full_ttl']]);
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    public function testNonExpiringViewWithNegativeTTL()
    {
        $views = new Tripod\Mongo\Composites\Views(
            $this->viewsConstParams[0],
            $this->viewsConstParams[1],
            $this->viewsConstParams[2]
        );

        $view = $views->getViewForResource(
            'http://talisaspire.com/events/1234',
            'v_event_no_expiration'
        );

        // should have no impact index and no _expires
        $expectedView = [
            '_id' => [
                'r' => 'http://talisaspire.com/events/1234',
                'c ' => 'http://talisaspire.com/',
                'type' => 'v_event_no_expiration',
            ],
            'value' => [
                _GRAPHS => [
                    [
                        '_id' => [
                            'r' => 'http://talisaspire.com/events/1234',
                            'c' => 'http://talisaspire.com/',
                        ],
                        'rdf:type' => ['u' => 'dctype:Event'],
                        'dct:references' => ['u' => 'http://talisaspire.com/resources/1234'],
                        'dct:created' => ['l' => '2018-04-09T00:00:00Z'],
                        'dct:title' => ['l' => 'A significant event'],
                    ],
                    [
                        '_id' => [
                            'r' => 'http://talisaspire.com/resources/1234',
                            'c' => 'http://talisaspire.com/',
                        ],
                        'dct:title' => ['l' => 'A real piece of work'],
                        'dct:creator' => ['l' => 'Anne Author'],
                    ],
                ],
            ],
        ];
        // get the view direct from mongo
        $actualView = Tripod\Config::getInstance()
            ->getCollectionForView('tripod_php_testing', 'v_event_no_expiration')
            ->findOne(
                [
                    '_id' => [
                        'r' => 'http://talisaspire.com/events/1234',
                        'c' => 'http://talisaspire.com/',
                        'type' => 'v_event_no_expiration',
                    ],
                ]
            );
        $this->assertEqualsCanonicalizing(
            $expectedView['_id'],
            $actualView['_id'],
            '_id does not match expected'
        );
        $this->assertContainsEquals(
            $expectedView['value'][_GRAPHS][0],
            $actualView['value'][_GRAPHS]
        );
        $this->assertContainsEquals(
            $expectedView['value'][_GRAPHS][1],
            $actualView['value'][_GRAPHS]
        );
        $this->assertCount(2, $actualView['value'][_GRAPHS]);
        $this->assertArrayNotHasKey(_EXPIRES, $actualView['value']);
        $this->assertArrayNotHasKey(_IMPACT_INDEX, $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);

        // Fetch the joined resource and change it
        $graph = $this->tripod->describeResource('http://talisaspire.com/resources/1234');

        $updatedGraph = new ExtendedGraph($graph->to_ntriples());
        $updatedGraph->replace_literal_triple(
            'http://talisaspire.com/resources/1234',
            'http://purl.org/dc/terms/title',
            'A real piece of work',
            'A literal treasure'
        );

        // This should not affect the view at all
        $this->tripod->saveChanges($graph, $updatedGraph);

        $view = $views->getViewForResource(
            'http://talisaspire.com/events/1234',
            'v_event_no_expiration'
        );

        // get the view direct from mongo, it should the same as earlier
        $actualView2 = Tripod\Config::getInstance()
            ->getCollectionForView('tripod_php_testing', 'v_event_no_expiration')
            ->findOne(
                [
                    '_id' => [
                        'r' => 'http://talisaspire.com/events/1234',
                        'c' => 'http://talisaspire.com/',
                        'type' => 'v_event_no_expiration',
                    ],
                ]
            );
        $this->assertEquals($actualView, $actualView2);
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
        $graph = $this->tripodViews->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'v_resource_to_single_source');
        foreach ($graph->get_resource_triple_values('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'http://purl.org/dc/terms/source') as $object) {
            $this->assertFalse($graph->get_subject_subgraph($object)->is_empty(), "Subgraph for {$object} should not be empty, should have been followed as join");
        }
    }

    public function testTTLViewIsRegeneratedOnFetch()
    {
        // make mock return expiry date in past...
        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getExpirySecFromNow'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time() - 300)));

        $mockTripodViews->generateView('v_resource_full_ttl', 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        // now mock out generate views and check it's called...
        $mockTripodViews2 = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews2->expects($this->once())->method('generateView')->with('v_resource_full_ttl', 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA');

        $mockTripodViews2->getViewForResource('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', 'v_resource_full_ttl');
    }

    public function testGenerateViewWithCountAggregate()
    {
        $expiryDate = Tripod\Mongo\DateUtil::getMongoDate((time() + 300) * 1000);

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getExpirySecFromNow'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->once())->method('getExpirySecFromNow')->with(300)->will($this->returnValue((time() + 300)));

        $mockTripodViews->getViewForResource('http://talisaspire.com/works/4d101f63c10a6', 'v_counts');

        $expectedView = [
            '_id' => [
                'r' => 'http://talisaspire.com/works/4d101f63c10a6',
                'c' => 'http://talisaspire.com/',
                'type' => 'v_counts'],
            'value' => [
                _GRAPHS => [
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6-2', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            ['u' => 'bibo:Book'],
                            ['u' => 'acorn:Work'],
                        ],
                        'acorn:resourceCount' => [
                            'l' => '0',
                        ],
                        'acorn:isbnCount' => [
                            'l' => '1',
                        ],
                    ],
                    [
                        '_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6', 'c' => 'http://talisaspire.com/'],
                        'rdf:type' => [
                            ['u' => 'bibo:Book'],
                            ['u' => 'acorn:Work'],
                        ],
                        'acorn:seeAlso' => [
                            'u' => 'http://talisaspire.com/works/4d101f63c10a6-2',
                        ],
                        'acorn:resourceCount' => [
                            'l' => '2',
                        ],
                        'acorn:resourceCountAlt' => [
                            'l' => '0',
                        ],
                        'acorn:isbnCount' => [
                            'l' => '2',
                        ],
                    ],
                ],
                _EXPIRES => $expiryDate,
            ],
        ];

        $actualView = Tripod\Config::getInstance()->getCollectionForView('tripod_php_testing', 'v_counts')->findOne(['_id' => ['r' => 'http://talisaspire.com/works/4d101f63c10a6', 'c' => 'http://talisaspire.com/', 'type' => 'v_counts']]);
        $this->assertEquals($expectedView['_id'], $actualView['_id']);
        $this->assertEquals($expectedView['value'], $actualView['value']);
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $actualView['_cts']);
    }

    public function testGetViewWithNamespaces()
    {
        $g = $this->tripodViews->getViewForResource('baseData:1', 'v_work_see_also', 'baseData:DefaultGraph');
        $this->assertFalse($g->is_empty(), 'Graph should not be empty');
        $this->assertTrue($g->get_subject_subgraph('http://talisaspire.com/works/4d101f63c10a6-2')->is_empty(), 'Graph for see also should be empty, as does not exist in requested context');

        $g2 = $this->tripodViews->getViewForResource('baseData:2', 'v_work_see_also', 'baseData:DefaultGraph');
        $this->assertFalse($g2->is_empty(), 'Graph should not be empty');
        $this->assertFalse($g2->get_subject_subgraph('http://basedata.com/b/2')->is_empty(), 'Graph for see also should be populated, as does exist in requested context');

        // use a mock heron-in to make sure generateView is not called again for different combinations of qname/full uri

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->never())->method('generateView');

        $g3 = $mockTripodViews->getViewForResource('http://basedata.com/b/2', 'v_work_see_also', 'http://basedata.com/b/DefaultGraph');
        $g4 = $mockTripodViews->getViewForResource('baseData:2', 'v_work_see_also', 'http://basedata.com/b/DefaultGraph');
        $g5 = $mockTripodViews->getViewForResource('http://basedata.com/b/2', 'v_work_see_also', 'baseData:DefaultGraph');
        $this->assertEquals($g2->to_ntriples(), $g3->to_ntriples(), 'View requested with subject/context qnamed should be equal to that with unnamespaced params');
        $this->assertEquals($g2->to_ntriples(), $g4->to_ntriples(), 'View requested with subject/context qnamed should be equal to that with only resource namespaced');
        $this->assertEquals($g2->to_ntriples(), $g5->to_ntriples(), 'View requested with subject/context qnamed should be equal to that with only context namespaced');
    }

    public function testGenerateViewsForResourcesOfTypeWithNamespace()
    {
        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(['ok' => true]));

        // spec is namespaced, acorn:Work, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType('http://talisaspire.com/schema#Work');

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs($this->viewsConstParams)
            ->getMock();
        $mockTripodViews->expects($this->atLeastOnce())->method('generateView')->will($this->returnValue(['ok' => true]));

        // spec is fully qualified, http://talisaspire.com/shema#Work2, can it resolve?
        $mockTripodViews->generateViewsForResourcesOfType('acorn:Work2');
    }

    // todo: more unit tests to cover other view spec/search document properties: condition, maxJoins, followSequence, from

    public function testGetViewForResourcesDoesNotInvokeViewGenerationForMissingResources()
    {
        $uri1 = 'http://uri1';
        $uri2 = 'http://uri2';

        $viewType = 'someView';
        $context = 'http://someContext';

        $query = [
            '_id' => [
                '$in' => [
                    ['r' => $uri1, 'c' => $context, 'type' => $viewType],
                    ['r' => $uri2, 'c' => $context, 'type' => $viewType],
                ],
            ],
        ];

        $returnedGraph = new ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1, 'http://somepred', 'someval');

        $mockDb = $this->getMockBuilder(MongoDB\Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'view'])
            ->onlyMethods(['findOne'])
            ->getMock();

        $mockDb->expects($this->any())->method('selectCollection')->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method('findOne')->will($this->returnValue(null));

        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD', 'getCollectionForView'])
            ->getMock();

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(Tripod\Config::getConfig());

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView', 'fetchGraph', 'getConfigInstance'])
            ->setConstructorArgs(['tripod_php_testing', $mockColl, $context])
            ->getMock();

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->once())
            ->method('fetchGraph')
            ->with($query, MONGO_VIEW, $mockViewColl, null, 101)
            ->will($this->returnValue($returnedGraph));

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $resultGraph = $mockTripodViews->getViewForResources([$uri1, $uri2], $viewType, $context);

        $this->assertEquals($returnedGraph->to_ntriples(), $resultGraph->to_ntriples());
    }

    public function testGetViewForResourcesInvokesViewGenerationForMissingResources()
    {
        $uri1 = 'http://uri1';
        $uri2 = 'http://uri2';

        $viewType = 'someView';
        $context = 'http://someContext';

        $mockDb = $this->getMockBuilder(MongoDB\Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'view'])
            ->onlyMethods(['findOne'])
            ->getMock();

        $mockDb->expects($this->any())->method('selectCollection')->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method('findOne')->will($this->returnValue(['_id' => $uri1])); // the actual returned doc is not important, it just has to not be null

        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD', 'getCollectionForView'])
            ->getMock();

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(Tripod\Config::getConfig());

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView', 'fetchGraph', 'getConfigInstance'])
            ->setConstructorArgs(['tripod_php_testing', $mockColl, $context])
            ->getMock();

        $mockTripodViews->expects($this->once())
            ->method('generateView')
            ->with($viewType, $uri2, $context)
            ->will($this->returnValue(['ok' => true]));

        $mockTripodViews->expects($this->exactly(2))
            ->method('fetchGraph')
            ->will($this->returnCallback([$this, 'fetchGraphInGetViewForResourcesCallback']));

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $resultGraph = $mockTripodViews->getViewForResources([$uri1, $uri2], $viewType, $context);

        $expectedGraph = new ExtendedGraph();
        $expectedGraph->add_literal_triple($uri1, 'http://somepred', 'someval');
        $expectedGraph->add_literal_triple($uri2, 'http://somepred', 'someval');

        $this->assertEquals($expectedGraph->to_ntriples(), $resultGraph->to_ntriples());
    }

    public function testDeletionOfResourceTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('dct:subject'),
            'Things grouped by no specific criteria'
        );

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => $context]);
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = Tripod\Config::getInstance()->getCollectionsForViews(
            'tripod_php_testing',
            ['v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source']
        );

        foreach ($collections as $collection) {
            $this->assertGreaterThan(
                0,
                $collection->count(['_id.r' => $labeller->uri_to_alias($uri1), '_id.c' => $context])
            );
        }

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri1) => [
                'rdf:type', 'searchterms:topic', 'dct:isVersionOf',
            ],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods(['getDataUpdater', 'getComposite'])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => $context,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods(['processSyncOperations', 'queueAsyncOperations'])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateViewsForResourcesOfType'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context,
            ])
            ->getMock();

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

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri1),
                    _ID_CONTEXT => $context,
                ],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_full_ttl, because TTL views don't include impactIndex
                [
                    'v_resource_full',
                    'v_resource_full_ttl',
                    'v_resource_to_single_source',
                    'v_resource_filter1',
                    'v_resource_filter2',
                    'v_resource_rdfsequence',
                ]
            ),
        ];

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects, '', 0.0, 10, true);

        foreach ($impactedSubjects as $subject) {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach ($collections as $collection) {
            $this->assertEquals(
                0,
                $collection->count(
                    ['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]
                )
            );
        }
    }

    /**
     * Basically identical to testDeletionOfResourceTriggersViewRegeneration, but focus on $url2, instead
     */
    public function testDeletionOfResourceInImpactIndexTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => $context]);
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = Tripod\Config::getInstance()->getCollectionsForViews('tripod_php_testing', ['v_resource_full', 'v_resource_to_single_source']);

        foreach ($collections as $collection) {
            $this->assertGreaterThan(0, $collection->count(['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]));
        }

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri2) => [
                'rdf:type', 'dct:subject',
            ],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater', 'getComposite',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => $context,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context,
            ])
            ->getMock();

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
                [
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ]
            );

        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), new ExtendedGraph());

        // Walk through the processSyncOperations process manually for views

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT => $context,
                ],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                ['v_resource_full', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence']
            ),
        ];

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach ($collections as $collection) {
            $this->assertEquals(0, $collection->count(['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]));
        }
    }

    /**
     * Basically identical to testDeletionOfResourceInImpactIndexTriggersViewRegeneration, but update $url2, rather
     * than deleting it
     */
    public function testUpdateOfResourceInImpactIndexTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => $context]);
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = Tripod\Config::getInstance()->getCollectionsForViews('tripod_php_testing', ['v_resource_full', 'v_resource_to_single_source']);

        foreach ($collections as $collection) {
            $this->assertGreaterThan(0, $collection->count(['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]));
        }

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri2) => ['dct:subject'],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater', 'getComposite',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => $context,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context,
            ])
            ->getMock();

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
                [
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ]
            );

        $newGraph = $originalGraph->get_subject_subgraph($uri2);
        $newGraph->replace_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria', 'Grab bag');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri2), $newGraph);

        // Walk through the processSyncOperations process manually for views

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT => $context,
                ],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                // Don't include v_resource_to_single_source because $url2 wouldn't be joined in it
                ['v_resource_full', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence']
            ),
        ];

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach ($collections as $collection) {
            $this->assertEquals(0, $collection->count(['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]));
        }
    }

    /**
     * Similar to testDeletionOfResourceTriggersViewRegeneration except $url1 is updated, rather than deleted
     */
    public function testUpdateOfResourceTriggersViewRegeneration()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('searchterms:topic'), 'Assorted things');

        $uri2 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri2, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri2, $labeller->qname_to_uri('dct:subject'), 'Things grouped by no specific criteria');

        $originalGraph->add_resource_triple($uri1, $labeller->qname_to_uri('dct:isVersionOf'), $uri2);
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => $context]);
        $tripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $collections = Tripod\Config::getInstance()->getCollectionsForViews('tripod_php_testing', ['v_resource_full', 'v_resource_full_ttl', 'v_resource_to_single_source']);

        foreach ($collections as $collection) {
            $this->assertGreaterThan(0, $collection->count(['_id.r' => $labeller->uri_to_alias($uri1), '_id.c' => $context]));
        }

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri1) => ['dct:title'],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater', 'getComposite',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => $context,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context,
            ])
            ->getMock();

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
                [
                    $this->equalTo('v_resource_full'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_full_ttl'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_to_single_source'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter1'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_filter2'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo('v_resource_rdfsequence'),
                    $this->equalTo($uri1),
                    $this->equalTo($context),
                ]
            );

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:title'), 'Title of Resource');
        $mockTripod->saveChanges($originalGraph->get_subject_subgraph($uri1), $newGraph);

        // Walk through the processSyncOperations process manually for views

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri1), // The impacted subject should still be $uri, since $uri2 is just in the impactIndex
                    _ID_CONTEXT => $context,
                ],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                ['v_resource_full', 'v_resource_to_single_source', 'v_resource_filter1', 'v_resource_filter2', 'v_resource_rdfsequence']
            ),
        ];

        $impactedSubjects = $view->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        foreach ($impactedSubjects as $subject) {
            $view->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated view.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        foreach ($collections as $collection) {
            $this->assertEquals(0, $collection->count(['value._impactIndex' => ['r' => $labeller->uri_to_alias($uri1), 'c' => $context]]));
        }
    }

    /**
     * Test that a change to a resource that isn't covered by a viewspec or in an impact index still triggers the discover
     * impacted subjects operation and returns nothing
     */
    public function testResourceUpdateNotCoveredBySpecStillTriggersOperations()
    {
        $context = 'http://talisaspire.com/';

        $labeller = new Tripod\Mongo\Labeller();
        // First add a graph
        $originalGraph = new ExtendedGraph();

        $uri1 = 'http://example.com/resources/' . uniqid();
        $originalGraph->add_resource_triple($uri1, RDF_TYPE, $labeller->qname_to_uri('bibo:Document'));
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:title'), 'How to speak American like a native');
        $originalGraph->add_literal_triple($uri1, $labeller->qname_to_uri('dct:subject'), 'Languages -- \'Murrican');

        $originalSubjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri1) => ['rdf:type', 'dct:title', 'dct:subject'],
        ];

        $updatedSubjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri1) => ['dct:subject'],
        ];

        $mockTripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
            ->onlyMethods([
                'getDataUpdater', 'getComposite',
            ])
            ->setConstructorArgs([
                'CBD_testing',
                'tripod_php_testing',
                [
                    'defaultContext' => $context,
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => true,
                        OP_VIEWS => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateViewsForResourcesOfType'])
            ->setConstructorArgs([
                'tripod_php_testing',
                Tripod\Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                $context,
            ])
            ->getMock();

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
                [
                    $this->equalTo($originalSubjectsAndPredicatesOfChange),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo($updatedSubjectsAndPredicatesOfChange),
                    $this->equalTo($context),
                ]
            );

        $mockTripodUpdates->expects($this->exactly(2))
            ->method('queueAsyncOperations')
            ->withConsecutive(
                [
                    $this->equalTo($originalSubjectsAndPredicatesOfChange),
                    $this->equalTo($context),
                ],
                [
                    $this->equalTo($updatedSubjectsAndPredicatesOfChange),
                    $this->equalTo($context),
                ]
            );

        $mockViews->expects($this->never())
            ->method('generateViewsForResourcesOfType');

        $mockTripod->saveChanges(new ExtendedGraph(), $originalGraph);

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $impactedSubjects = $view->getImpactedSubjects($originalSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

        $newGraph = $originalGraph->get_subject_subgraph($uri1);
        $newGraph->replace_literal_triple($uri1, $labeller->qname_to_uri('dct:subject'), 'Languages -- \'Murrican', 'Languages -- English, American');

        $mockTripod->saveChanges($originalGraph, $newGraph);

        $view = $mockTripod->getComposite(OP_VIEWS);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Views::class, $view);

        $impactedSubjects = $view->getImpactedSubjects($updatedSubjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);

    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject()
    {
        $tripod = $this->getMockBuilder(Tripod\Mongo\Driver::class)
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

        $tripodUpdates = $this->getMockBuilder(Tripod\Mongo\Updates::class)
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
            ->will($this->returnValue($tripodUpdates));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new Tripod\Mongo\MongoGraph();
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

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Journal'));  // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:title'), 'This is yet another new resource');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'art');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'design');
        $subjectsAndPredicatesOfChange = [
            $newSubjectUri1 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri2 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri3 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
        ];
        $tripod->saveChanges(new Tripod\Mongo\MongoGraph(), $g);

        $views = $tripod->getComposite(OP_VIEWS);

        $expectedImpactedSubjects = [
            new Tripod\Mongo\ImpactedSubject(
                [
                    _ID_RESOURCE => $newSubjectUri2,
                    _ID_CONTEXT => 'http://talisaspire.com/',
                ],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                []
            ),
        ];

        $impactedSubjects = $views->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testSavingToAPreviouslyEmptySeqeunceUpdatesView()
    {
        // create a tripod with views sync
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', [
            'defaultContext' => 'http://talisaspire.com/',
            'async' => [OP_VIEWS => false],
        ]);

        // should be no triples with "http://basedata.com/b/sequence123" as subject in existing view
        $view = $tripod->getViewForResource('http://basedata.com/b/docWithEmptySeq123', 'v_doc_with_seqeunce');
        $this->assertTrue($view->has_triples_about('http://basedata.com/b/docWithEmptySeq123'));
        $this->assertFalse($view->has_triples_about('http://basedata.com/b/sequence123'));

        $newGraph = new ExtendedGraph();
        $newGraph->add_resource_to_sequence('http://basedata.com/b/sequence123', 'http://basedata.com/b/sequenceItem123');

        $tripod->saveChanges(new ExtendedGraph(), $newGraph);

        // should be triples with "http://basedata.com/b/sequence123" as subject in new view
        $view = $this->tripod->getViewForResource('http://basedata.com/b/docWithEmptySeq123', 'v_doc_with_seqeunce');
        $this->assertTrue($view->has_triples_about('http://basedata.com/b/docWithEmptySeq123'));
        $this->assertTrue($view->has_triples_about('http://basedata.com/b/sequence123'));
    }

    public function testSavingToAPreviouslyEmptyJoinUpdatesView()
    {
        // create a tripod with views sync
        $tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', [
            'defaultContext' => 'http://talisaspire.com/',
            'async' => [OP_VIEWS => false],
        ]);

        // should be no triples with "http://basedata.com/b/sequence123" as subject in existing view
        $view = $tripod->getViewForResource('http://basedata.com/b/docWithEmptySeq123', 'v_doc_with_seqeunce');
        $this->assertTrue($view->has_triples_about('http://basedata.com/b/docWithEmptySeq123'));
        $this->assertFalse($view->has_triples_about('http://schemas.talis.com/2005/user/schema#xyz'));

        $newGraph = new ExtendedGraph();
        $newGraph->add_literal_triple('http://schemas.talis.com/2005/user/schema#xyz', 'http://rdfs.org/sioc/spec/name', 'Some name');

        $tripod->saveChanges(new ExtendedGraph(), $newGraph);

        // should be triples with "http://basedata.com/b/sequence123" as subject in new view
        $view = $tripod->getViewForResource('http://basedata.com/b/docWithEmptySeq123', 'v_doc_with_seqeunce');
        $this->assertTrue($view->has_triples_about('http://basedata.com/b/docWithEmptySeq123'));
        $this->assertTrue($view->has_triples_about('http://schemas.talis.com/2005/user/schema#xyz'));
    }

    /**
     * @return ExtendedGraph
     */
    public function fetchGraphInGetViewForResourcesCallback()
    {
        $uri1 = 'http://uri1';
        $uri2 = 'http://uri2';

        $viewType = 'someView';
        $context = 'http://someContext';

        $query1 = ['_id' => ['$in' => [['r' => $uri1, 'c' => $context, 'type' => $viewType], ['r' => $uri2, 'c' => $context, 'type' => $viewType]]]];
        $query2 = ['_id' => ['$in' => [['r' => $uri2, 'c' => $context, 'type' => $viewType]]]];

        $returnedGraph1 = new ExtendedGraph();
        $returnedGraph1->add_literal_triple($uri1, 'http://somepred', 'someval');

        $returnedGraph2 = new ExtendedGraph();
        $returnedGraph2->add_literal_triple($uri2, 'http://somepred', 'someval');

        $args = func_get_args();
        if ($args[0] == $query1) {
            return $returnedGraph1;
        }
        if ($args[0] == $query2) {
            return $returnedGraph2;
        }
            $this->fail();

    }

    public function testCursorNoExceptions()
    {
        $uri1 = 'http://uri1';

        $viewType = 'someView';
        $context = 'http://someContext';

        $returnedGraph = new ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1, 'http://somepred', 'someval');

        $mockDb = $this->getMockBuilder(MongoDB\Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'view'])
            ->onlyMethods(['find'])
            ->getMock();
        $mockCursor = $this->getMockBuilder(ArrayIterator::class)
            ->onlyMethods(['rewind'])
            ->getMock();

        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

        $mockDb->expects($this->any())->method('selectCollection')->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method('findOne')->will($this->returnValue(null));

        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD', 'getCollectionForView'])
            ->getMock();

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(Tripod\Config::getConfig());

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView', 'getConfigInstance'])
            ->setConstructorArgs(['tripod_php_testing', $mockColl, $context])
            ->getMock();

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $mockTripodViews->getViewForResources([$uri1], $viewType, $context);
    }

    public function testCursorExceptionThrown()
    {
        $uri1 = 'http://uri1';

        $viewType = 'someView';
        $context = 'http://someContext';

        $returnedGraph = new ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1, 'http://somepred', 'someval');

        $mockDb = $this->getMockBuilder(MongoDB\Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'view'])
            ->onlyMethods(['findOne', 'find'])
            ->getMock();
        $mockCursor = $this->getMockBuilder(ArrayIterator::class)
            ->onlyMethods(['rewind'])
            ->getMock();

        $mockCursor->expects($this->exactly(30))->method('rewind')->will($this->throwException(new Exception('Exception thrown when cursoring to Mongo')));
        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

        $mockDb->expects($this->any())->method('selectCollection')->will($this->returnValue($mockColl));
        $mockColl->expects($this->never())->method('findOne');

        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD', 'getCollectionForView'])
            ->getMock();

        $mockConfig->expects($this->never())
            ->method('getCollectionForCBD');

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(Tripod\Config::getConfig());

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView', 'getConfigInstance'])
            ->setConstructorArgs(['tripod_php_testing', $mockColl, $context])
            ->getMock();

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Exception thrown when cursoring to Mongo');
        $mockTripodViews->getViewForResources([$uri1], $viewType, $context);
    }

    public function testCursorNoExceptionThrownWhenCursorThrowsSomeExceptions()
    {
        $uri1 = 'http://uri1';

        $viewType = 'someView';
        $context = 'http://someContext';

        $returnedGraph = new ExtendedGraph();
        $returnedGraph->add_literal_triple($uri1, 'http://somepred', 'someval');

        $mockDb = $this->getMockBuilder(MongoDB\Database::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['selectCollection'])
            ->getMock();
        $mockColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['findOne'])
            ->getMock();
        $mockViewColl = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'view'])
            ->onlyMethods(['find', 'findOne'])
            ->getMock();
        $mockCursor = $this->getMockBuilder(ArrayIterator::class)
            ->onlyMethods(['rewind'])
            ->getMock();

        $mockCursor->expects($this->exactly(5))
            ->method('rewind')
            ->will($this->onConsecutiveCalls(
                $this->throwException(new Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new Exception('Exception thrown when cursoring to Mongo')),
                $this->throwException(new Exception('Exception thrown when cursoring to Mongo')),
                $this->returnValue($mockCursor)
            ));

        $mockViewColl->expects($this->once())->method('find')->will($this->returnValue($mockCursor));

        $mockDb->expects($this->any())->method('selectCollection')->will($this->returnValue($mockColl));
        $mockColl->expects($this->once())->method('findOne')->will($this->returnValue(null));

        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForCBD', 'getCollectionForView'])
            ->getMock();

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockColl));

        $mockConfig->expects($this->atLeastOnce())
            ->method('getCollectionForView')
            ->with('tripod_php_testing', $this->anything(), $this->anything())
            ->will($this->returnValue($mockViewColl));

        $mockConfig->loadConfig(Tripod\Config::getConfig());

        $mockTripodViews = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['generateView', 'getConfigInstance'])
            ->setConstructorArgs(['tripod_php_testing', $mockColl, $context])
            ->getMock();

        $mockTripodViews->expects($this->never())
            ->method('generateView');

        $mockTripodViews->expects($this->atLeastOnce())
            ->method('getConfigInstance')
            ->will($this->returnValue($mockConfig));

        $mockTripodViews->getViewForResources([$uri1], $viewType, $context);
    }

    public function testCountViews()
    {
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getCollectionForViewSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $views->expects($this->once())
            ->method('getCollectionForViewSpec')
            ->with('v_some_spec')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with(['_id.type' => 'v_some_spec'])
            ->will($this->returnValue(101));

        $this->assertEquals(101, $views->count('v_some_spec'));
    }

    public function testCountViewsWithFilters()
    {
        $filters = ['_cts' => ['$lte' => new MongoDB\BSON\UTCDateTime(null)]];
        $query = array_merge(['_id.type' => 'v_some_spec'], $filters);
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getCollectionForViewSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $views->expects($this->once())
            ->method('getCollectionForViewSpec')
            ->with('v_some_spec')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('count')
            ->with($query)
            ->will($this->returnValue(101));

        $this->assertEquals(101, $views->count('v_some_spec', $filters));
    }

    public function testDeleteViewsByViewId()
    {
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(MongoDB\DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->will($this->returnValue(30));

        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getCollectionForViewSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $views->expects($this->once())
            ->method('getCollectionForViewSpec')
            ->with('v_resource_full')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with(['_id.type' => 'v_resource_full'])
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(30, $views->deleteViewsByViewId('v_resource_full'));
    }

    public function testDeleteViewsByViewIdWithTimestamp()
    {
        $timestamp = new MongoDB\BSON\UTCDateTime(null);

        $query = [
            '_id.type' => 'v_resource_full',
            '$or' => [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ],
        ];
        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(MongoDB\DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->will($this->returnValue(30));

        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getCollectionForViewSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $views->expects($this->once())
            ->method('getCollectionForViewSpec')
            ->with('v_resource_full')
            ->will($this->returnValue($collection));

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with($query)
            ->will($this->returnValue($deleteResult));

        $this->assertEquals(30, $views->deleteViewsByViewId('v_resource_full', $timestamp));
    }

    public function testBatchViewGeneration()
    {
        $count = 234;
        $docs = [];

        $configOptions = json_decode(file_get_contents(__DIR__ . '/data/config.json'), true);

        for ($i = 0; $i < $count; $i++) {
            $docs[] = ['_id' => ['r' => 'tenantLists:batch' . $i, 'c' => 'tenantContexts:DefaultGraph']];
        }

        $fakeCursor = new ArrayIterator($docs);
        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForView', 'getCollectionForCBD'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig($configOptions);

        $collection = $this->getMockBuilder(MongoDB\Collection::class)
            ->onlyMethods(['count', 'find'])
            ->setConstructorArgs([new MongoDB\Driver\Manager(), 'db', 'coll'])
            ->getMock();
        $collection->expects($this->atLeastOnce())->method('count')->willReturn($count);
        $collection->expects($this->atLeastOnce())->method('find')->willReturn($fakeCursor);

        $configInstance->expects($this->atLeastOnce())->method('getCollectionForCBD')->willReturn($collection);

        $views = $this->getMockBuilder(Tripod\Mongo\Composites\Views::class)
            ->onlyMethods(['getConfigInstance', 'queueApplyJob'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'tenantContexts:DefaultGraph'])
            ->getMock();
        $views->expects($this->atLeastOnce())->method('getConfigInstance')->willReturn($configInstance);
        $views->expects($this->exactly(10))->method('queueApplyJob')
            ->withConsecutive(
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(25)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf('\Tripod\Mongo\ImpactedSubject'),
                        $this->countOf(9)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ]
            );
        $views->generateView('v_resource_full', null, null, 'TESTQUEUE');
    }
}
