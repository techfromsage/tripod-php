<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';
require_once 'src/mongo/queue/MongoTripodQueue.class.php';

class MongoTripodQueueTest extends MongoTripodTestBase
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
        $this->indexQueue->addItem(ModifiedSubject::create(array('r'=>'http://example.com/1','c'=>'http://talisaspire.com/'), array('http://talisaspire.com/schema#Resource'), array(OP_TABLES,OP_SEARCH), array(), 'myDB', 'myCollection'));

        // retrieve it and assert properties
        $data = $this->indexQueue->fetchNextQueuedItem()->getData();

        $this->assertEquals('myDB', $data['database']);
        $this->assertEquals('myCollection', $data['collection']);
        $this->assertEquals('http://example.com/1', $data['r']);
        $this->assertEquals(array('http://talisaspire.com/schema#Resource'), $data['rdf:type']);
        $this->assertEquals('processing', $data['status']);
        $this->assertArrayHasKey('_id', $data);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);
    }

    public function testRemoveFromIndexQueue()
    {
        // add an item, verify that its there
        $this->indexQueue->addItem(ModifiedSubject::create(array('r'=>'http://example.com/2','c'=>'http://talisaspire.com/'), array('http://talisaspire.com/schema#Resource'), array(OP_TABLES,OP_SEARCH), array(), 'myDB', 'myCollection'));
        $modifiedSubject = $this->indexQueue->fetchNextQueuedItem();
        $data = $modifiedSubject->getData();
        $this->assertEquals('http://example.com/2', $data['r']);
        $this->assertEquals(array('http://talisaspire.com/schema#Resource'), $data['rdf:type']);
        $this->assertEquals('processing', $data['status']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);

        $_id = $data['_id'];
        $this->indexQueue->removeItem($modifiedSubject);
        $this->assertNull($this->indexQueue->getItem($_id));
    }

    public function testUpdateQueuedItemStatusToFailed()
    {
        // add an item, verify that its there
        $this->indexQueue->addItem(ModifiedSubject::create(array('r'=>'http://example.com/3','c'=>'http://talisaspire.com/'), array('http://talisaspire.com/schema#Resource'), array(OP_TABLES,OP_SEARCH), array(), 'myDB', 'myCollection'));
        $modifiedSubject = $this->indexQueue->fetchNextQueuedItem();
        $data = $modifiedSubject->getData();
        $this->assertEquals('http://example.com/3', $data['r']);
        $this->assertEquals(array('http://talisaspire.com/schema#Resource'), $data['rdf:type']);
        $this->assertEquals('processing', $data['status']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);

        $_id = $data['_id'];

        // mark the item as failed, with an error message
        $this->indexQueue->failItem($modifiedSubject, "Something went wrong");

        // retrieve the item and assert
        $failedItem = $this->indexQueue->getItem($_id);
        $this->assertEquals('http://example.com/3', $failedItem['r']);
        $this->assertEquals('failed', $failedItem['status']);
        $this->assertEquals('Something went wrong', $failedItem['errorMessage']);
        $this->assertInstanceOf('MongoDate', $failedItem['createdOn']);
        $this->assertInstanceOf('MongoDate', $failedItem['lastUpdated']);
    }

    public function testQueueStateTransitions()
    {
        $itemId = 'qid_test';

        $mockQueue = $this->getMock('MongoTripodQueue', array('getUniqId'), array());
        $mockQueue->expects($this->any())->method('getUniqId')->will($this->returnValue($itemId));

        $mockQueue->addItem(ModifiedSubject::create(array('r'=>'http://example.com/test','c'=>'http://talisaspire.com/'), array('http://talisaspire.com/schema#Resource'), array(OP_TABLES,OP_SEARCH), array(), 'myDB', 'myCollection'));
        $item = $mockQueue->getItem($itemId);

        $this->assertEquals('myDB', $item['database']);
        $this->assertEquals('myCollection', $item['collection']);
        $this->assertEquals('http://example.com/test', $item['r']);
        $this->assertEquals(array('http://talisaspire.com/schema#Resource'), $item['rdf:type']);
        $this->assertContains('qid_', $item['_id']);
        $this->assertInstanceOf('MongoDate', $item['createdOn']);
        $this->assertArrayNotHasKey('lastUpdated', $item);
        $this->assertEquals('queued', $item['status']);

        $modifiedSubject = $mockQueue->fetchNextQueuedItem();
        $data=$modifiedSubject->getData();
        $this->assertEquals('myDB', $data['database']);
        $this->assertEquals('myCollection', $data['collection']);
        $this->assertEquals('http://example.com/test', $data['r']);
        $this->assertEquals(array('http://talisaspire.com/schema#Resource'), $data['rdf:type']);
        $this->assertContains('qid_', $data['_id']);
        $this->assertInstanceOf('MongoDate', $data['createdOn']);
        $this->assertInstanceOf('MongoDate', $data['lastUpdated']);
        $this->assertEquals('processing', $data['status']);

        $mockQueue->failItem($modifiedSubject, "oops");
        $item = $mockQueue->getItem($itemId);
        $this->assertEquals('failed', $item['status']);
        $this->assertEquals('oops', $item['errorMessage']);

        $mockQueue->removeItem($modifiedSubject);
        $this->assertNull($mockQueue->getItem($itemId));
    }

    public function testProcessNextCallsObserversUpdateForViews()
    {
        // set up the queue item
        $queuedItem = new ModifiedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_VIEWS),
            "createdOn"=>new MongoDate()
        ));

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
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
    public function testProcessNextCallsObserversUpdateForTables()
    {
        // set up the queue item
        $queuedItem = new ModifiedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_TABLES),
            "createdOn"=>new MongoDate()
        ));

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
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
    public function testProcessNextCallsObserversUpdateForSearch()
    {
        // set up the queue item
        $queuedItem = new ModifiedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_SEARCH),
            "createdOn"=>new MongoDate()
        ));

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockSearchIndexer = $this->getMock("MongoTripodSearchIndexer",array("update"),array($this->tripod));

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
    public function testProcessNextCallsObserversUpdateForAll()
    {
        // set up the queue item
        $queuedItem = new ModifiedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(OP_SEARCH,OP_TABLES,OP_VIEWS),
            "createdOn"=>new MongoDate()
        ));

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
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
    public function testProcessNextCallsObserversUpdateForNone()
    {
        // set up the queue item
        $queuedItem = new ModifiedSubject(array(
            "_id"=>'blah',
            "r"=>"http://talisaspire.com/works/4d101f63c10a6",
            "c"=>'http://talisaspire.com/',
            "database"=>'tripod_php_testing',
            "collection"=>'CBD_testing',
            "operations"=>array(),
            "createdOn"=>new MongoDate()
        ));

        // mock tripod and observers
        $mockTripod = $this->getMock(
            "MongoTripod",
            array("getTripodTables","getSearchIndexer","getTripodViews"),
            array('CBD_testing', 'tripod_php_testing', array('defaultContext'=>'http://talisaspire.com/'))
        );
        $mockViews = $this->getMock(
            "MongoTripodViews",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
        );
        $mockTables = $this->getMock(
            "MongoTripodTables",
            array("update"),
            array($this->tripod->getGroup(),$this->getTripodCollection($this->tripod),"http://talisaspire.com/")
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