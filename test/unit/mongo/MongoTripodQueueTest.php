<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';

/**
 * todo: refactor this whole class as queue is dead.
 */
class MongoTripodQueueDeadTestRefactor
{
    /**
     * @var MongoTripod
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransactionLog = null;

    /**
     * @var MongoTripodQueue
     */
    private $indexQueue=null;

    protected function setUp()
    {
        parent::setup();
        //Mongo::setPoolSize(200);

        $this->indexQueue = new MongoTripodQueue();
        $this->indexQueue->purgeQueue();

        $this->tripod = new MongoTripod('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/'));

    }

    /* BASIC tests */

    public function testAddToIndexQueue()
    {
        // add item to queue
        $this->indexQueue->addItem(new ChangeSet(),array(),"foo","CBD_wibble",array(OP_VIEWS));

        // retrieve it and assert properties
        $data = $this->indexQueue->fetchNextQueuedItem();

        $this->assertArrayHasKey('_id', $data);
        $this->assertEquals('processing', $data['status']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);
    }

    //todo: maybe a duplicate of test in TripodQueueOperations
    public function testRemoveFromIndexQueue()
    {
        // add an item, verify that its there
        $this->indexQueue->addItem(new ChangeSet(),array(),"foo","CBD_wibble",array(OP_VIEWS));
        $data = $this->indexQueue->fetchNextQueuedItem();
        $this->assertEquals('processing', $data['status']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);

        $_id = $data['_id'];

        $this->indexQueue->removeItem($data);
        $this->assertNull($this->indexQueue->getItem($_id));
    }

    //todo: maybe a duplicate of test in TripodQueueOperations
    public function testUpdateQueuedItemStatusToFailed()
    {
        // add an item, verify that its there
        $this->indexQueue->addItem(new ChangeSet(),array(),"foo","CBD_wibble",array(OP_VIEWS));
        $data = $this->indexQueue->fetchNextQueuedItem();
        $this->assertEquals('processing', $data['status']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);

        $_id = $data['_id'];

        // mark the item as failed, with an error message
        $this->indexQueue->failItem($data, "Something went wrong");

        // retrieve the item and assert
        $failedItem = $this->indexQueue->getItem($_id);
        $this->assertEquals('failed', $failedItem['status']);
        $this->assertEquals('Something went wrong', $failedItem['errorMessage']);
        $this->assertInstanceOf('MongoDate', $failedItem['createdOn']);
        $this->assertInstanceOf('MongoDate', $failedItem['lastUpdated']);
    }

    //todo: maybe a duplicate of test in TripodQueueOperations
    public function testQueueStateTransitions()
    {
        $itemId = 'qid_test';

        /* @var $mockQueue MongoTripodQueue */
        $mockQueue = $this->getMock('MongoTripodQueue', array('getUniqId'), array());
        $mockQueue->expects($this->any())->method('getUniqId')->will($this->returnValue($itemId));

        $mockQueue->addItem(new ChangeSet(),array(),"foo","CBD_wibble",array(OP_VIEWS));
        $item = $mockQueue->getItem($itemId);

        $this->assertContains('qid_', $item['_id']);
        $this->assertInstanceOf('MongoDate', $item['createdOn']);
        $this->assertArrayNotHasKey('lastUpdated', $item);
        $this->assertEquals('queued', $item['status']);

        $data = $mockQueue->fetchNextQueuedItem();

        $this->assertContains('qid_', $data['_id']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);
        $this->assertInstanceOf('MongoDate', $data['lastUpdated']);
        $this->assertEquals('processing', $data['status']);

        $mockQueue->failItem($data, "oops");
        $item = $mockQueue->getItem($itemId);

        $this->assertEquals('failed', $item['status']);
        $this->assertEquals('oops', $item['errorMessage']);

        $mockQueue->removeItem($item);
        $this->assertNull($mockQueue->getItem($itemId));
    }

    // todo: work out what the funk to do with this...
    public function xtestProcessNextCallsObserversUpdateForViews()
    {
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        // set up the queue item
        $queuedItem = new ImpactedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_VIEWS),
            "createdOn"=>new MongoDate()),
            $mockViews
        );

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

        // get tripod to return mock observers
        $mockTripod->expects($this->any())->method('getTripodViews')->will($this->returnValue($mockViews));
        $mockTripod->expects($this->any())->method('getTripodTables')->will($this->returnValue($mockTables));
        $mockTripod->expects($this->any())->method('getSearchIndexer')->will($this->returnValue($mockSearchIndexer));

        // expect what should be called, and what shouldn't
        $mockViews->expects($this->once())->method('update');
        $mockTables->expects($this->never())->method('update');
        $mockSearchIndexer->expects($this->never())->method('update');

        // run the queue
        $mockQueue = $this->getMock('MongoTripodQueue', array('fetchNextQueuedItem','getMongoTripod'), array());
        $mockQueue->expects($this->any())->method('fetchNextQueuedItem')->will($this->returnValue($queuedItem));
        $mockQueue->expects($this->any())->method('getMongoTripod')->will($this->returnValue($mockTripod));

        $mockQueue->processNext();
    }
    public function xtestProcessNextCallsObserversUpdateForTables()
    {
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );

        // set up the queue item
        $queuedItem = new ImpactedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_TABLES),
            "createdOn"=>new MongoDate()),
            $mockTables
        );

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

        // get tripod to return mock observers
        $mockTripod->expects($this->any())->method('getTripodViews')->will($this->returnValue($mockViews));
        $mockTripod->expects($this->any())->method('getTripodTables')->will($this->returnValue($mockTables));
        $mockTripod->expects($this->any())->method('getSearchIndexer')->will($this->returnValue($mockSearchIndexer));

        // expect what should be called, and what shouldn't
        $mockViews->expects($this->never())->method('update');
        $mockTables->expects($this->once())->method('update');
        $mockSearchIndexer->expects($this->never())->method('update');

        // run the queue
        $mockQueue = $this->getMock('MongoTripodQueue', array('fetchNextQueuedItem','getMongoTripod'), array());
        $mockQueue->expects($this->any())->method('fetchNextQueuedItem')->will($this->returnValue($queuedItem));
        $mockQueue->expects($this->any())->method('getMongoTripod')->will($this->returnValue($mockTripod));

        $mockQueue->processNext();
    }
    public function xtestProcessNextCallsObserversUpdateForSearch()
    {
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

        // set up the queue item
        $queuedItem = new ImpactedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_SEARCH),
            "createdOn"=>new MongoDate()),
            $mockSearchIndexer
        );

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );

        // get tripod to return mock observers
        $mockTripod->expects($this->any())->method('getTripodViews')->will($this->returnValue($mockViews));
        $mockTripod->expects($this->any())->method('getTripodTables')->will($this->returnValue($mockTables));
        $mockTripod->expects($this->any())->method('getSearchIndexer')->will($this->returnValue($mockSearchIndexer));

        // expect what should be called, and what shouldn't
        $mockViews->expects($this->never())->method('update');
        $mockTables->expects($this->never())->method('update');
        $mockSearchIndexer->expects($this->once())->method('update');

        // run the queue
        $mockQueue = $this->getMock('MongoTripodQueue', array('fetchNextQueuedItem','getMongoTripod'), array());
        $mockQueue->expects($this->any())->method('fetchNextQueuedItem')->will($this->returnValue($queuedItem));
        $mockQueue->expects($this->any())->method('getMongoTripod')->will($this->returnValue($mockTripod));

        $mockQueue->processNext();
    }
    public function xtestProcessNextCallsObserversUpdateForAll()
    {
        // set up the queue item
        $queuedItem = new ImpactedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_SEARCH,OP_TABLES,OP_VIEWS),
            "createdOn"=>new MongoDate()),
            null
        );

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

        // get tripod to return mock observers
        $mockTripod->expects($this->any())->method('getTripodViews')->will($this->returnValue($mockViews));
        $mockTripod->expects($this->any())->method('getTripodTables')->will($this->returnValue($mockTables));
        $mockTripod->expects($this->any())->method('getSearchIndexer')->will($this->returnValue($mockSearchIndexer));

        // expect what should be called, and what shouldn't
        $mockViews->expects($this->once())->method('update');
        $mockTables->expects($this->once())->method('update');
        $mockSearchIndexer->expects($this->once())->method('update');

        // run the queue
        $mockQueue = $this->getMock('MongoTripodQueue', array('fetchNextQueuedItem','getMongoTripod'), array());
        $mockQueue->expects($this->any())->method('fetchNextQueuedItem')->will($this->returnValue($queuedItem));
        $mockQueue->expects($this->any())->method('getMongoTripod')->will($this->returnValue($mockTripod));

        $mockQueue->processNext();
    }
    public function xtestProcessNextCallsObserversUpdateForNone()
    {
        // set up the queue item
        $queuedItem = new ImpactedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(),
            "createdOn"=>new MongoDate()),
            null
        );

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

        // get tripod to return mock observers
        $mockTripod->expects($this->any())->method('getTripodViews')->will($this->returnValue($mockViews));
        $mockTripod->expects($this->any())->method('getTripodTables')->will($this->returnValue($mockTables));
        $mockTripod->expects($this->any())->method('getSearchIndexer')->will($this->returnValue($mockSearchIndexer));

        // expect what should be called, and what shouldn't
        $mockViews->expects($this->never())->method('update');
        $mockTables->expects($this->never())->method('update');
        $mockSearchIndexer->expects($this->never())->method('update');

        // run the queue
        $mockQueue = $this->getMock('MongoTripodQueue', array('fetchNextQueuedItem','getMongoTripod'), array());
        $mockQueue->expects($this->any())->method('fetchNextQueuedItem')->will($this->returnValue($queuedItem));
        $mockQueue->expects($this->any())->method('getMongoTripod')->will($this->returnValue($mockTripod));

        $mockQueue->processNext();
    }
}