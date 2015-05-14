<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';

/**
 * Class MongoTripodQueueOperationsTest
 * IMPORTANT NOTE:  this test suite does not use any MOCKING, each test will hit your local mongodb instance.
 *
 * This test suite verifies, for a number of different scenarios, that when we save changes through tripod the correct number of items are added to the
 * Tripod Queue, and for each of those items added to the queue the correct operations are listed; furthermore in some cases that when operations are performed
 * the results are as we would expect. For that reason this suite is more than just a series of unit tests, feels more like a set of integration tests since we
 * are testing a chained flow of events.
 * todo: refactor this whole lot due to resque
 */
class MongoTripodQueueOperations extends MongoTripodTestBase
{
    /**
     * @var MongoTripod
     */
    protected $tripod = null;

    protected function setUp()
    {
        parent::setup();
        $this->tripod = new MongoTripod(
            'CBD_testing',
            'tripod_php_testing'
        );
        $this->getTripodCollection($this->tripod)->drop();
        $this->loadBaseDataViaTripod();
    }

    /**
     * Saving a change to a single resource that does not impact any other resources should result in just a single item being added to the queue.
     */
    public function testSingleItemIsAddedToQueueForChangeToSingleSubject()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getDataUpdater', 'getComposite'))
            ->setConstructorArgs(
                array(
                    'CBD_testing',
                    'tripod_php_testing',
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)
                    )
                )
            )->getMock();

        $tripodUpdates = $this->getMockBuilder('MongoTripodUpdates')
            ->setMethods(array('processSyncOperations','submitJob'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)
                )
            ))->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        $subjectsAndPredicatesOfChange = array(
            "http://talisaspire.com/resources/doc1"=>array('dct:subject')
        );

        $tripod->expects($this->never())
            ->method('getComposite');

        $data = array(
            "changes" => $subjectsAndPredicatesOfChange,
            "operations" => array(OP_VIEWS, OP_TABLES, OP_SEARCH),
            "tripodConfig" => MongoTripodConfig::getConfig(),
            "storeName" => 'tripod_php_testing',
            "podName" => 'CBD_testing',
            "contextAlias" => 'http://talisaspire.com/'
        );

        $tripodUpdates->expects($this->once())
            ->method('submitJob')
            ->with(MongoTripodConfig::getDiscoverQueueName(),"DiscoverImpactedSubjects",$data);


        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple(
            "http://talisaspire.com/resources/doc1",
            $g2->qname_to_uri("dct:subject"),
            "astrophysics"
        );

        $tripod->saveChanges($g1, $g2);
    }

    /**
     * Saving a change to a single resource that does not impact any other resources should result in just a single item being added to the queue.
     */
    public function testSingleItemWithViewsOpIsAddedToQueueForChangeToSingleSubject()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getDataUpdater', 'getComposite'))
            ->setConstructorArgs(
                array(
                    'CBD_testing',
                    'tripod_php_testing',
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>false, OP_SEARCH=>false)
                    )
                )
            )->getMock();

        $tripodUpdates = $this->getMockBuilder('MongoTripodUpdates')
            ->setMethods(array('processSyncOperations','submitJob'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>false, OP_SEARCH=>false)
                )
            ))->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        $subjectsAndPredicatesOfChange = array(
            "http://talisaspire.com/resources/doc1"=>array('dct:subject')
        );

        $tripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange, 'http://talisaspire.com/'
            );

        $data = array(
            "changes" => $subjectsAndPredicatesOfChange,
            "operations" => array(OP_VIEWS),
            "tripodConfig" => MongoTripodConfig::getConfig(),
            "storeName" => 'tripod_php_testing',
            "podName" => 'CBD_testing',
            "contextAlias" => 'http://talisaspire.com/'
        );

        $tripodUpdates->expects($this->once())
            ->method('submitJob')
            ->with(MongoTripodConfig::getDiscoverQueueName(),"DiscoverImpactedSubjects",$data);


        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple(
            "http://talisaspire.com/resources/doc1",
            $g2->qname_to_uri("dct:subject"),
            "astrophysics"
        );

        $tripod->saveChanges($g1, $g2);
    }

    public function testNoItemIsAddedToQueueForChangeToSingleSubjectWithNoAsyncOps()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getDataUpdater', 'getComposite'))
            ->setConstructorArgs(
                array(
                    'CBD_testing',
                    'tripod_php_testing',
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)
                    )
                )
            )->getMock();

        $tripodUpdates = $this->getMockBuilder('MongoTripodUpdates')
            ->setMethods(array('processSyncOperations','submitJob'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    OP_ASYNC=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)
                )
            ))->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        $subjectsAndPredicatesOfChange = array(
            "http://talisaspire.com/resources/doc1"=>array('dct:subject')
        );

        $tripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange, 'http://talisaspire.com/'
            );


        $tripodUpdates->expects($this->never())
            ->method('submitJob');


        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple(
            "http://talisaspire.com/resources/doc1",
            $g2->qname_to_uri("dct:subject"),
            "astrophysics"
        );

        $tripod->saveChanges($g1, $g2);
    }

    /**
     * Saving a change to an entity that appears in the impact index for view/table_rows/search docs of 3 other entities should result in
     * 4 items being placed on the queue, with the operations for each relevant to the configured operations based on the specifications
     * todo: new test in composite for one subject that impacts another
     */
    public function testSingleJobSubmittedToQueueForChangeToSeveralSubjects()
    {
        $tripod = $this->getMockBuilder('MongoTripod')
            ->setMethods(array('getDataUpdater', 'getComposite'))
            ->setConstructorArgs(
                array(
                    'CBD_testing',
                    'tripod_php_testing',
                    array(
                        'defaultContext'=>'http://talisaspire.com/',
                        OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)
                    )
                )
            )->getMock();

        $tripodUpdates = $this->getMockBuilder('MongoTripodUpdates')
            ->setMethods(array('processSyncOperations','submitJob'))
            ->setConstructorArgs(array(
                $tripod,
                array(
                    'defaultContext'=>'http://talisaspire.com/',
                    OP_ASYNC=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)
                )
            ))->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->will($this->returnValue($tripodUpdates));

        $subjectsAndPredicatesOfChange = array(
            "http://talisaspire.com/resources/doc1"=>array('dct:date'),
            "http://talisaspire.com/resources/doc2"=>array('dct:date'),
            "http://talisaspire.com/resources/doc3"=>array('dct:date'),
        );

        $tripod->expects($this->never())
            ->method('getComposite');

        $data = array(
            "changes" => $subjectsAndPredicatesOfChange,
            "operations" => array(OP_VIEWS, OP_TABLES, OP_SEARCH),
            "tripodConfig" => MongoTripodConfig::getConfig(),
            "storeName" => 'tripod_php_testing',
            "podName" => 'CBD_testing',
            "contextAlias" => 'http://talisaspire.com/'
        );

        $tripodUpdates->expects($this->once())
            ->method('submitJob')
            ->with(MongoTripodConfig::getDiscoverQueueName(),"DiscoverImpactedSubjects",$data);

        $g1 = $tripod->describeResources(array(
            "http://talisaspire.com/resources/doc1",
            "http://talisaspire.com/resources/doc2",
            "http://talisaspire.com/resources/doc3"
        ));
        $g2 = $tripod->describeResources(array(
            "http://talisaspire.com/resources/doc1",
            "http://talisaspire.com/resources/doc2",
            "http://talisaspire.com/resources/doc3"
        ));

        $g2->add_literal_triple("http://talisaspire.com/resources/doc1", $g2->qname_to_uri("dct:date"),"01-01-1970" );
        $g2->add_literal_triple("http://talisaspire.com/resources/doc2", $g2->qname_to_uri("dct:date"),"01-01-1970" );
        $g2->add_literal_triple("http://talisaspire.com/resources/doc3", $g2->qname_to_uri("dct:date"),"01-01-1970" );

        $tripod->saveChanges($g1, $g2);
   }
}