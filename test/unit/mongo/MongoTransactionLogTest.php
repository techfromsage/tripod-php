<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/Driver.class.php';
require_once 'src/mongo/delegates/TransactionLog.class.php';
require_once 'src/mongo/MongoGraph.class.php';

/**
 * Class MongoTransactionLogTest
 */
class MongoTransactionLogTest extends MongoTripodTestBase
{
    /**
     * @var \Tripod\Mongo\Driver
     */
    protected $tripod;

    /**
     * @var \Tripod\Mongo\TransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function setUp()
    {
        parent::setup();
        //Mongo::setPoolSize(200);

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $tripod */
        $this->tripod = $this->getMock('\Tripod\Mongo\Driver', array('addToSearchIndexQueue'), array('CBD_testing','tripod_php_testing'));
        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->getTripodCollection($this->tripod)->drop();

        // Lock collection no longer available from Driver, so drop it manually
        \Tripod\Mongo\Config::getInstance()->getCollectionForLocks($this->tripod->getStoreName())->drop();

        $this->loadResourceDataViaTripod();

        $this->tripodTransactionLog = new \Tripod\Mongo\TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

    }

    public function testReplayLog_Simple_ChangesSinglePropertyOnExistingDocument()
    {
        $uri = 'http://talisaspire.com/examples/1';

        // store a new entity
        $originalGraph = new \Tripod\Mongo\MongoGraph();
        $originalGraph->add_resource_triple($uri, $originalGraph->qname_to_uri('rdf:type'), $originalGraph->qname_to_uri("acorn:Resource"));
        $originalGraph->add_literal_triple($uri, $originalGraph->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $originalGraph->add_literal_triple($uri, $originalGraph->qname_to_uri('searchterms:author'), 'Joe Bloggs');
        $this->tripod->saveChanges(new \Tripod\ExtendedGraph(), $originalGraph, "http://talisaspire.com/");
        // jsut confirm the values we just added were set in the store
        $oG = $this->tripod->describeResource($uri);

        $this->assertTrue($oG->has_resource_triple($uri, $oG->qname_to_uri('rdf:type'), $oG->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($oG->has_literal_triple($uri, $oG->qname_to_uri('searchterms:author'), 'Joe Bloggs'), "Graph should contain literal triple we added");
        $this->assertTrue($oG->has_literal_triple($uri, $oG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition'), "Graph should  contain literal triple we addded");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0); // the document should be at version 0

        // now save a change which alters one property
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($originalGraph);
        $nG->remove_literal_triple($uri, $nG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $nG->add_literal_triple($uri, $nG->qname_to_uri('searchterms:title'), 'TEST TITLE');

        // save change to the entity and make sure it was applied ( this will result in a transaction being generated )
        $this->tripod->saveChanges($originalGraph, $nG, "http://talisaspire.com/", 'my changes');
        $uG = $this->tripod->describeResource($uri);

        $this->assertTrue($uG->has_resource_triple($uri, $uG->qname_to_uri('rdf:type'), $uG->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:title'), 'TEST TITLE'), "Graph should contain literal triple we added");
        $this->assertTrue($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:author'), 'Joe Bloggs'), "Graph should contain literal triple we added");
        $this->assertFalse($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition'), "Graph should not contain literal triple we removed");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);
        $uG = null;

        // drop the collection so we no longer have anything in the CBD
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction
        $this->tripod->replayTransactionLog();

        // assert that the entity matches the version after we changed it
        $uG = $this->tripod->describeResource($uri);

        $this->assertTrue($uG->has_resource_triple($uri, $uG->qname_to_uri('rdf:type'), $uG->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:title'), 'TEST TITLE'), "Graph should contain literal triple we added");
        $this->assertTrue($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:author'), 'Joe Bloggs'), "Graph should contain literal triple we added");
        $this->assertFalse($uG->has_literal_triple($uri, $uG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition'), "Graph should not contain literal triple we removed");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);
    }

    public function testReplayLog_Simple_AddSingleNewDocument()
    {
        $uri = 'http://example.com/resources/1';

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g, "http://talisaspire.com/", "something new");

        // make sure the new entity was saved correctly
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0);

        // drop the collection so CBD collection is empty
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction log, which should contain a single transaction
        $this->tripod->replayTransactionLog();

        // assert that the CBD collection contains the document we expect and it has the correct version
        $uG = $this->tripod->describeResource($uri);
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0);
    }

    public function testReplayLog_Simple_AddSingleNewDocumentNamespacedResourceAndContext()
    {
        $uri = 'http://basedata.com/b/3';

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $this->tripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g, "http://basedata.com/b/DefaultGraph", "something new");

        // make sure the new entity was saved correctly
        $uG = $this->tripod->describeResource($uri,"baseData:DefaultGraph");
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"baseData:DefaultGraph"), 0);

        // drop the collection so CBD collection is empty
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction log, which should contain a single transaction
        $this->tripod->replayTransactionLog();

        // assert that the CBD collection contains the document we expect and it has the correct version
        $uG = $this->tripod->describeResource($uri,"http://basedata.com/b/DefaultGraph");
        $this->assertTrue($uG->has_triples_about($uri), "new entity we created was not saved");
        $this->assertTrue($uG->has_resource_triple( $uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource")), "Graph should contain resource triple we added");
        $this->assertTrue($uG->has_literal_triple( $uri, $g->qname_to_uri("dct:title"), "wibble"), "Graph should contain literal triple we added");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://basedata.com/b/DefaultGraph"), 0);
    }

    /**
     * In this test we have a document in the store which we delete.
     * The transaction log records the fact it was deleted.
     * We then drop the collection and re-add the original document.
     * We replay the transaction log to ensure that it does physically remove the document.
     */
    public function testReplayLog_Simple_DeletesSingleEntity()
    {
        // document is added via setup() method
        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA';
        $oG = $this->tripod->describeResource($uri);

        // save change which effectively removes the entire entity.
        $this->tripod->saveChanges($oG, new \Tripod\ExtendedGraph(),"http://talisaspire.com/");
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));

        // drop the collection so CBD collection is empty
        $this->getTripodCollection($this->tripod)->drop();

        // re-add the base data, verify document exists in the store
        $this->loadResourceData();
        $this->assertDocumentExists(array("r"=>$uri,"c"=>"http://talisaspire.com/"));

        // replay the transaction
        $this->tripod->replayTransactionLog();

        // document should have been removed
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));
    }

    /**
     * This test is different to the one above.
     * In this case we start with a document in the store.
     * We modify the entity creating a transaction.
     * We then remove the entity, which generates another transaction.
     * We then clear the db, and replay the transaction log, against an empty DB
     *
     * Since we step backwards through the transaction log. This test verifies that if a more recent
     * transaction deleted an entity then an older transaction should not re-introduce it.
     */
    public function testReplayLog_Simple_DeletesSingleEntityWithNoBaseData()
    {
        // document is added via setup() method
        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA';

        // Save a change to entity
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_literal_triple($uri, $oG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_literal_triple($uri, $oG->qname_to_uri('searchterms:title'), 'A different title');
        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);

        // now delete the entity & confirm
        $oG = $this->tripod->describeResource($uri);
        $this->tripod->saveChanges($oG, new \Tripod\ExtendedGraph(),"http://talisaspire.com/");
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));

        // transaction log should have 2 transactions in it at this point.
        $this->assertEquals(2, $this->tripodTransactionLog->getTotalTransactionCount());

        // drop the collection so CBD collection is empty
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction
        $this->tripod->replayTransactionLog();

        // document should have been removed
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));
    }

    public function testReplayTransactionsOverSeveralDocuments_AddingAndUpdating()
    {
        // base documents added via setup() method
        $uri_1 = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA';
        $uri_2 = 'http://talisaspire.com/works/4d101f63c10a6';

        // save a changes that results in a changeset with more than one subject of change
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_literal_triple($uri_1, $oG->qname_to_uri('searchterms:title'), 'Physics 3rd Edition');
        $oG->add_literal_triple($uri_2, $oG->qname_to_uri('searchterms:discipline'), 'physics');
        $oG->add_resource_triple($uri_2, $oG->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/physics');
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_literal_triple($uri_1, $nG->qname_to_uri('searchterms:title'), 'History of UK');
        $nG->add_literal_triple($uri_2, $nG->qname_to_uri('searchterms:discipline'), 'history');
        $nG->add_resource_triple($uri_2, $nG->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/history');
        // save changes.
        $this->tripod->saveChanges($oG, $nG,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri_1,"c"=>"http://talisaspire.com/"), 1);
        $this->assertDocumentVersion(array("r"=>$uri_2,"c"=>"http://talisaspire.com/"), 1);
        $this->assertEquals(1, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 1 transaction in the transaction log');

        // add a title to second entity:
        $nG2  = new \Tripod\Mongo\MongoGraph();
        $nG2->add_literal_triple($uri_2, $nG2->qname_to_uri('searchterms:title'), 'some test title');
        // this save should add a triple to just one of the documents
        $this->tripod->saveChanges(new \Tripod\ExtendedGraph(), $nG2,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri_1,"c"=>"http://talisaspire.com/"), 1); // this document should not have been changed
        $this->assertDocumentVersion(array("r"=>$uri_2,"c"=>"http://talisaspire.com/"), 2);
        $this->assertEquals(2, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 2 transactions in the transaction log');

        // change same entity again:
        $nG2  = new \Tripod\Mongo\MongoGraph();
        $nG2->add_literal_triple($uri_2, $nG2->qname_to_uri('searchterms:title'), 'some test title');
        $nG3  = new \Tripod\Mongo\MongoGraph();
        $nG3->add_literal_triple($uri_2, $nG2->qname_to_uri('searchterms:title'), 'a different title');

        // this save should change a triple on on document
        $this->tripod->saveChanges($nG2, $nG3,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri_1,"c"=>"http://talisaspire.com/"), 1); // this document should not have been changed
        $this->assertDocumentVersion(array("r"=>$uri_2,"c"=>"http://talisaspire.com/"), 3);
        $this->assertEquals(3, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 3 transactions in the transaction log');

        // change the other entity once more so it has a second revision
        // change same entity again:
        $nG2  = new \Tripod\Mongo\MongoGraph();
        $nG2->add_literal_triple($uri_1, $nG2->qname_to_uri('searchterms:title'), 'History of UK');
        $nG3  = new \Tripod\Mongo\MongoGraph();
        $nG3->add_literal_triple($uri_1, $nG2->qname_to_uri('searchterms:title'), 'History of the United Kingdom');

        // this save should change a triple on the first resource
        $this->tripod->saveChanges($nG2, $nG3,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri_1,"c"=>"http://talisaspire.com/"), 2);
        $this->assertDocumentVersion(array("r"=>$uri_2,"c"=>"http://talisaspire.com/"), 3);
        $this->assertEquals(4, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 4 transactions in the transaction log');

        // drop store, replay transactions
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction log, which should contain a single transaction
        $this->tripod->replayTransactionLog();
        $this->assertDocumentVersion(array("r"=>$uri_1,"c"=>"http://talisaspire.com/"), 2);
        $this->assertDocumentVersion(array("r"=>$uri_2,"c"=>"http://talisaspire.com/"), 3);

        // assert the two resources are as we expect them to be.
        $resource1 = $this->tripod->describeResource($uri_1);
        $this->assertHasLiteralTriple($resource1, $uri_1, $resource1->qname_to_uri('searchterms:title'), 'History of the United Kingdom');

        $resource2 = $this->tripod->describeResource($uri_2);
        $this->assertHasLiteralTriple($resource2, $uri_2, $resource1->qname_to_uri('searchterms:title'), 'a different title');
        $this->assertHasLiteralTriple($resource2, $uri_2, $resource1->qname_to_uri('searchterms:discipline'), 'history');
        $this->assertHasResourceTriple($resource2, $uri_2, $resource1->qname_to_uri('dct:subject'), 'http://talisaspire.com/disciplines/history');
    }

    public function testReplayTransactions_AddingAndDeleting()
    {
        $uri = 'http://example.com/resources/1';
        $g= new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($uri, $g->qname_to_uri('searchterms:title'), 'Anything at all');

        // add the entity to the store
        $this->tripod->saveChanges(new \Tripod\ExtendedGraph(), $g,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0);
        $this->assertEquals(1, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 1 transaction in the transaction log');

        // delete the entity from the store
        $this->tripod->saveChanges($g, new \Tripod\ExtendedGraph(),"http://talisaspire.com/");
        $this->assertDocumentHasBeenDeleted(array("r"=>$uri,"c"=>"http://talisaspire.com/"));
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1);
        $this->assertEquals(2, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 2 transactions in the transaction log');

        // add it again ( slightly different document for assertion)
        $g->add_literal_triple($uri, $g->qname_to_uri('searchterms:isbn'), '1234567890');
        $this->tripod->saveChanges(new \Tripod\ExtendedGraph(), $g,"http://talisaspire.com/");
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 2);
        $this->assertEquals(3, $this->tripodTransactionLog->getTotalTransactionCount(), 'There should only be 3 transaction in the transaction log');

        // drop the collection so CBD collection is empty
        $this->getTripodCollection($this->tripod)->drop();

        // replay the transaction
        $this->tripod->replayTransactionLog();

        $uG = $this->tripod->describeResource($uri);
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 2);
        $this->assertHasLiteralTriple($uG, $uri, $uG->qname_to_uri('searchterms:title'), 'Anything at all');
        $this->assertHasLiteralTriple($uG, $uri, $uG->qname_to_uri('searchterms:isbn'), '1234567890');
    }


    /**
     * This test addes a set of precanned transactions to the transaction log, and then verifies that
     * only the transactions $gte the given date are replayed
     */
    public function testReplayTransactionsFromAGivenDate()
    {

        $transaction_1 = $this->buildTransactionDocument(1, 'http://example.com/resources/1', '2013-01-21T13:00:00.000Z', '2013-01-21T13:01:00.000Z', 0);
        $transaction_2 = $this->buildTransactionDocument(2, 'http://example.com/resources/2', '2013-01-21T13:00:00.000Z', '2013-01-21T13:02:00.000Z', 0);
        $transaction_3 = $this->buildTransactionDocument(3, 'http://example.com/resources/3', '2013-01-21T13:00:00.000Z', '2013-01-21T13:03:00.000Z', 0);
        $transaction_4 = $this->buildTransactionDocument(4, 'http://example.com/resources/4', '2013-01-21T13:00:00.000Z', '2013-01-21T13:04:00.000Z', 0);
        $transaction_5 = $this->buildTransactionDocument(5, 'http://example.com/resources/5', '2013-01-21T13:00:00.000Z', '2013-01-21T13:05:00.000Z', 0);

        $this->addDocument($transaction_1, true);
        $this->addDocument($transaction_2, true);
        $this->addDocument($transaction_3, true);
        $this->addDocument($transaction_4, true);
        $this->addDocument($transaction_5, true);

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->replayTransactionLog("2013-01-21T13:03:00.000Z");

        $g = $this->tripod->describeResources(array(
            'http://example.com/resources/1',
            'http://example.com/resources/2',
            'http://example.com/resources/3',
            'http://example.com/resources/4',
            'http://example.com/resources/5',
        ));

        $this->assertFalse($g->has_triples_about('http://example.com/resources/1'), "Should not contain anything about /resources/1");
        $this->assertFalse($g->has_triples_about('http://example.com/resources/2'), "Should not contain anything about /resources/2");
        $this->assertTrue( $g->has_triples_about('http://example.com/resources/3'), "Should contain triples about /resources/3");
        $this->assertTrue( $g->has_triples_about('http://example.com/resources/4'), "Should contain triples about /resources/4");
        $this->assertTrue( $g->has_triples_about('http://example.com/resources/5'), "Should contain triples about /resources/5");
    }

    /**
     * This test addes a set of precanned transactions to the transaction log, and then verifies that
     * only the transactions $gte the given date are replayed
     */
    public function testReplayTransactionsBetweenTwoDates()
    {

        $transaction_1 = $this->buildTransactionDocument(1, 'http://example.com/resources/1', '2013-01-21T13:00:00.000Z', '2013-01-21T13:01:00.000Z', 0);
        $transaction_2 = $this->buildTransactionDocument(2, 'http://example.com/resources/2', '2013-01-21T13:00:00.000Z', '2013-01-21T13:02:00.000Z', 0);
        $transaction_3 = $this->buildTransactionDocument(3, 'http://example.com/resources/3', '2013-01-21T13:00:00.000Z', '2013-01-21T13:03:00.000Z', 0);
        $transaction_4 = $this->buildTransactionDocument(4, 'http://example.com/resources/4', '2013-01-21T13:00:00.000Z', '2013-01-21T13:04:00.000Z', 0);
        $transaction_5 = $this->buildTransactionDocument(5, 'http://example.com/resources/5', '2013-01-21T13:00:00.000Z', '2013-01-21T13:05:00.000Z', 0);

        $this->addDocument($transaction_1, true);
        $this->addDocument($transaction_2, true);
        $this->addDocument($transaction_3, true);
        $this->addDocument($transaction_4, true);
        $this->addDocument($transaction_5, true);

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->replayTransactionLog("2013-01-21T13:02:00.000Z", "2013-01-21T13:04:00.000Z");

        $g = $this->tripod->describeResources(array(
            'http://example.com/resources/1',
            'http://example.com/resources/2',
            'http://example.com/resources/3',
            'http://example.com/resources/4',
            'http://example.com/resources/5',
        ));

        $this->assertFalse($g->has_triples_about('http://example.com/resources/1'), "Should not contain anything about /resources/1");
        $this->assertTrue($g->has_triples_about('http://example.com/resources/2'), "Should contain triples about /resources/2");
        $this->assertTrue( $g->has_triples_about('http://example.com/resources/3'), "Should contain triples about /resources/3");
        $this->assertTrue( $g->has_triples_about('http://example.com/resources/4'), "Should contain triples about /resources/4");
        $this->assertFalse( $g->has_triples_about('http://example.com/resources/5'), "Should not contain triples about /resources/5");
    }

    /**
     * helper method
     * @param string $id
     * @param string $subjectOfChange
     * @param string $startTime
     * @param string $endTime
     * @param int $_version
     * @return array
     */
    protected function buildTransactionDocument($id, $subjectOfChange, $startTime, $endTime, $_version)
    {
        $transaction_template = array(
            '_id' => "transaction_{$id}",
            'changes' => array(
                array(
                    '_id' => array('r' => '_:cs0', 'c'=>'http://talisaspire.com/'),
                    'rdf:type' => array('u' => 'cs:ChangeSet'),
                    'cs:subjectOfChange' => array('u' => $subjectOfChange),
                    'cs:createdDate' => array('l' => date('c')),
                    'cs:creatorName' => array('l' => 'Unit Test'),
                    'cs:changeReason' => array('l' => 'Unit Test'),
                    'cs:addition' => array('u' => '_:Add1'),
                ),
                array(
                    '_id' => array('r' => '_:Add1', 'c'=>'http://talisaspire.com/'),
                    'rdf:type' => array('u' => 'rdf:Statement'),
                    'rdf:subject' => array('u' => $subjectOfChange),
                    'rdf:predicate' => array('u' => "searchterms:title"),
                    'rdf:object' => array('l' => "anything at all"),
                )
            ),
            'collectionName' => 'CBD_testing',
            'dbName' => 'tripod_php_testing',
            'startTime' => new MongoDate(strtotime($startTime)),
            'endTime' => new MongoDate(strtotime($endTime)),
            'status' => 'completed',
            'newCBDs' => array(array(
                "_id" => array('r' => $subjectOfChange, 'c' => 'http://talisaspire.com/'),
                "searchterms:title" => array('l' => 'anything at all'),
                "_version" => $_version,
                "_uts" => new MongoDate(),
                "_cts" => new MongoDate(),
            )),
            'originalCBDs' => array(array(
                "_id" => array('r' => $subjectOfChange, 'c' => 'http://talisaspire.com/')
            )),
        );

        return $transaction_template;
    }

    public function testTransactionIsLoggedCorrectlyWhenCompletedSuccessfully()
    {
        // STEP 1
        $uri = 'http://example.com/resources/1';
        // save a new entity, and retrieve it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");

        $mTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing', 'tripod_php_testing'));
        $mTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getUniqId'), array($mTripod));

        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('getUniqId')
            ->will($this->returnValue(1));
        $mTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mTripodUpdate));

        $mTripod->setTransactionLog($this->tripodTransactionLog);
        $mTripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g, 'http://talisaspire.com/');

        // assert that the transaction in the transaction log is correct
        $transactionId = 'transaction_1';
        $transactionDocument = $this->getDocument($transactionId, $this->tripod, true);
        $this->assertEquals($transactionId, $transactionDocument['_id'], 'transtion should have the mocked id we injected');
        $this->assertEquals('completed', $transactionDocument['status'], 'status of the transaction should be completed');
        $this->assertEquals(1, count($transactionDocument['originalCBDs']), 'There should only be on CBD in the originalCBs collection');
        $this->assertTransactionDate($transactionDocument, 'startTime');
        $this->assertTransactionDate($transactionDocument, 'endTime');
        $this->assertTrue(isset($transactionDocument['changes']), "Transaction should contain changes");
        $this->assertChangesForGivenSubject($transactionDocument['changes'], $uri, 2, 0);
        $expectedCBD = array('_id'=>array('r'=>'http://example.com/resources/1', 'c'=>'http://talisaspire.com/'));
        $actualCBD = $transactionDocument['originalCBDs'][0];
        $this->assertEquals($expectedCBD, $actualCBD,'CBD in transaction should match our expected value exactly');

        // STEP 2
        // update the same entity with an addition
        $mTripod = null;
        $mTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing', 'tripod_php_testing'));
        $mTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getUniqId'), array($mTripod));

        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('getUniqId')
            ->will($this->returnValue(2));
        $mTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mTripodUpdate));
        $mTripod->setTransactionLog($this->tripodTransactionLog);

        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g);
        $nG->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "another title");
        $mTripod->saveChanges($g, $nG,'http://talisaspire.com/');
        // assert this transaction is correct
        $transactionId = 'transaction_2';
        $transactionDocument = $this->getDocument($transactionId, $this->tripod, true);
        $this->assertEquals($transactionId, $transactionDocument['_id'], 'transtion should have the mocked id we injected');
        $this->assertEquals('completed', $transactionDocument['status'], 'status of the transaction should be completed');
        $this->assertEquals(1, count($transactionDocument['originalCBDs']), 'There should only be on CBD in the originalCBs collection');
        $this->assertTransactionDate($transactionDocument, 'startTime');
        $this->assertTransactionDate($transactionDocument, 'endTime');
        $this->assertTrue(isset($transactionDocument['changes']), "Transaction should contain changes");
        $this->assertChangesForGivenSubject($transactionDocument['changes'], $uri, 1, 0);
        $expectedCBD = array(
            '_id'=>array('r'=>'http://example.com/resources/1', 'c'=>'http://talisaspire.com/'),
            '_version'=>0,
            "dct:title"=>array('l'=>'wibble'),
            "rdf:type"=>array('u'=>"acorn:Resource")
        );
        $actualCBD = $transactionDocument['originalCBDs'][0];

        // TODO: find a better way of doing this, for now we set the expected created ts and updated ts to the same as actual or test will fail
        $expectedCBD[_UPDATED_TS] = $actualCBD[_UPDATED_TS];
        $expectedCBD[_CREATED_TS] = $actualCBD[_CREATED_TS];

        // sort the keys in both arrays to ensure equals check works (it will fail if the keys are in a different order)
        ksort($actualCBD);
        ksort($expectedCBD);

        $this->assertEquals($expectedCBD, $actualCBD,'CBD in transaction should match our expected value exactly');

        // STEP 3
        // update the same entity with a removal
        $mTripod = null;
        $mTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing', 'tripod_php_testing'));
        $mTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getUniqId'), array($mTripod));

        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('getUniqId')
            ->will($this->returnValue(3));
        $mTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mTripodUpdate));
        $mTripod->setTransactionLog($this->tripodTransactionLog);

        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_graph($nG);
        $g->remove_literal_triple($uri, $g->qname_to_uri("dct:title"), "another title");
        $mTripod->saveChanges($nG, $g,'http://talisaspire.com/');
        // assert this transaction
        $transactionId = 'transaction_3';
        $transactionDocument = $this->getDocument($transactionId, $this->tripod, true);
        $this->assertEquals($transactionId, $transactionDocument['_id'], 'transtion should have the mocked id we injected');
        $this->assertEquals('completed', $transactionDocument['status'], 'status of the transaction should be completed');
        $this->assertEquals(1, count($transactionDocument['originalCBDs']), 'There should only be on CBD in the originalCBs collection');
        $this->assertTransactionDate($transactionDocument, 'startTime');
        $this->assertTransactionDate($transactionDocument, 'endTime');
        $this->assertTrue(isset($transactionDocument['changes']), "Transaction should contain changes");
        $this->assertChangesForGivenSubject($transactionDocument['changes'], $uri, 0, 1);
        $expectedCBD = array(
            '_id'=>array('r'=>'http://example.com/resources/1','c'=>'http://talisaspire.com/'),
            '_version'=>1,
            "dct:title"=>array(array('l'=>'wibble'),array('l'=>'another title')),
            "rdf:type"=>array('u'=>"acorn:Resource")
        );
        $actualCBD = $transactionDocument['originalCBDs'][0];
        // TODO: find a better way of doing this, for now we set the expected created ts and updated ts to the same as actual or test will fail
        $expectedCBD[_UPDATED_TS] = $actualCBD[_UPDATED_TS];
        $expectedCBD[_CREATED_TS] = $actualCBD[_CREATED_TS];

        // sort the keys in both arrays to ensure equals check works (it will fail if the keys are in a different order)
        ksort($actualCBD);
        ksort($expectedCBD);
        $this->assertEquals($expectedCBD, $actualCBD,'CBD in transaction should match our expected value exactly');
    }

    public function testTransactionIsLoggedCorrectlyWhenSaveFails()
    {
        $uri = 'http://example.com/resources/1';
        // save a new entity, and retrieve it
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("acorn:Resource"));
        $g->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "wibble");
        $mTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing', 'tripod_php_testing'));
        $mTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getUniqId'), array($mTripod));

        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('getUniqId')
            ->will($this->returnValue(1));
        $mTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mTripodUpdate));

        $mTripod->setTransactionLog($this->tripodTransactionLog);
        $mTripod->saveChanges(new \Tripod\Mongo\MongoGraph(), $g, 'http://talisaspire.com/');

        // STEP 2
        // now attempt to update the entity but throw an exception in applyChangeset
        // this should cause the save to fail, and this should be reflected in the transaction log
        $mTripod = null;
        $mTripod = $this->getMock('\Tripod\Mongo\Driver', array('getDataUpdater'), array('CBD_testing', 'tripod_php_testing'));
        $mTripodUpdate = $this->getMock('\Tripod\Mongo\Updates', array('getUniqId', 'applyChangeSet'), array($mTripod));

        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('getUniqId')
            ->will($this->returnValue(2));
        $mTripodUpdate->expects($this->atLeastOnce())
            ->method('applyChangeSet')
            ->will($this->throwException(new Exception("exception thrown by mock test")));

        $mTripod->expects($this->atLeastOnce())
            ->method('getDataUpdater')
            ->will($this->returnValue($mTripodUpdate));

        $mTripod->setTransactionLog($this->tripodTransactionLog);
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_graph($g);
        $nG->add_literal_triple($uri, $g->qname_to_uri("dct:title"), "another title");

        try
        {
            $saved = $mTripod->saveChanges($g, $nG, 'http://talisaspire.com/');
            $this->fail("Exception should have been thrown");
        }
        catch (\Tripod\Exceptions\Exception $e)
        {
            // Squash exception here as we want to keep running assertions below.
        }

        // Now assert that the transaction logged the failure
        $transactionId = 'transaction_2';
        $transactionDocument = $this->getDocument($transactionId, $this->tripod, true);
        $this->assertEquals($transactionId, $transactionDocument['_id'], 'transtion should have the mocked id we injected');
        $this->assertEquals('failed', $transactionDocument['status'], 'status of the transaction should be failed');
        $this->assertEquals(1, count($transactionDocument['originalCBDs']), 'There should only be on CBD in the originalCBs collection');
        $this->assertTransactionDate($transactionDocument, 'startTime');
        $this->assertTransactionDate($transactionDocument, 'failedTime');
        $this->assertTrue(isset($transactionDocument['changes']), "Transaction should contain changes");
        $this->assertChangesForGivenSubject($transactionDocument['changes'], $uri, 1, 0);
        $expectedCBD = array(
            '_id'=>array('r'=>'http://example.com/resources/1','c'=>'http://talisaspire.com/'),
            '_version'=>0,
            "dct:title"=>array('l'=>'wibble'),
            "rdf:type"=>array('u'=>"acorn:Resource")
        );
        $actualCBD = $transactionDocument['originalCBDs'][0];
        // TODO: find a better way of doing this, for now we set the expected created ts and updated ts to the same as actual or test will fail
        $expectedCBD[_UPDATED_TS] = $actualCBD[_UPDATED_TS];
        $expectedCBD[_CREATED_TS] = $actualCBD[_CREATED_TS];

        // sort the keys in both arrays to ensure equals check works (it will fail if the keys are in a different order)
        ksort($actualCBD);
        ksort($expectedCBD);
        $this->assertEquals($expectedCBD, $actualCBD,'CBD in transaction should match our expected value exactly');
        // finally check that the actual error was logged in the transaction_log
        $this->assertTrue(isset($transactionDocument['error']) && isset($transactionDocument['error']['reason']) && isset($transactionDocument['error']['trace']),'The error should be logged, both the message and a stack trace');
        $this->assertEquals('exception thrown by mock test', $transactionDocument['error']['reason'], 'The transaction log should have logged the exception our test suite threw');
        $this->assertNotEmpty($transactionDocument['error']['trace'], 'The transaction log have a non empty error trace');
    }

    public function testTransactionsLoggedCorrectlyFromMultipleTripods()
    {
        // Create two tripods onto different collection/dbname and make them use the same transaction log
        $tripod1 = $this->getMock('\Tripod\Mongo\Driver', array('generateViewsAndSearchDocumentsForResources'), array('CBD_testing','tripod_php_testing'));
        $tripod1->expects($this->any())->method('generateViewsAndSearchDocumentsForResources');
        $this->getTripodCollection($tripod1)->drop();
        $tripod1->setTransactionLog($this->tripodTransactionLog);

        $tripod2 = $this->getMock('\Tripod\Mongo\Driver', array('generateViewsAndSearchDocumentsForResources'), array('CBD_testing_2','tripod_php_testing'));
        $tripod2->expects($this->any())->method('generateViewsAndSearchDocumentsForResources');
        $this->getTripodCollection($tripod2)->drop();
        $tripod2->setTransactionLog($this->tripodTransactionLog);

        $uri = 'http://example.com/resources/1';
        $g = new \Tripod\Mongo\MongoGraph();
        $g->add_literal_triple($uri, $g->qname_to_uri('searchterms:title'), "Some title");
        $g->add_literal_triple($uri, $g->qname_to_uri('searchterms:author'), "Some author");

        // save entity using both tripods ( creates same doc in two different collections )
        $tripod1->saveChanges(new \Tripod\ExtendedGraph(), $g, 'http://talisaspire.com/');
        $tripod2->saveChanges(new \Tripod\ExtendedGraph(), $g, 'http://talisaspire.com/');

        // assert the document is in both collections
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0, true, $tripod1);
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0, true, $tripod2);

        // assert the transaction log contains two transactions
        $this->assertEquals(2, $this->tripodTransactionLog->getTotalTransactionCount());

        // change one of the documents
        $oG = new \Tripod\Mongo\MongoGraph();
        $oG->add_literal_triple($uri, $g->qname_to_uri('searchterms:title'), "Some title");
        $nG = new \Tripod\Mongo\MongoGraph();
        $nG->add_literal_triple($uri, $g->qname_to_uri('searchterms:title'), "Changed title");
        $tripod1->saveChanges($oG,$nG,'http://talisaspire.com/');

        // assert the documents and transaction count
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 1, true, $tripod1);
        $this->assertDocumentVersion(array("r"=>$uri,"c"=>"http://talisaspire.com/"), 0, true, $tripod2);
        $this->assertEquals(3, $this->tripodTransactionLog->getTotalTransactionCount());
    }

    /**
     * This test ensures that if insertTransaction returns an error, then a Exception is actually thrown
     */
    public function testCreateNewTransactionThrowsExceptionIfInsertFails()
    {
        $mockInsert = $this->getMockBuilder('\MongoDB\InsertOneResult')
            ->disableOriginalConstructor()
            ->setMethods(['isAcknowledged'])
            ->getMock();
        $mockInsert
            ->expects($this->once())
            ->method('isAcknowledged')
            ->will($this->returnValue(false));

        $mockTransactionLog = $this->getMock('\Tripod\Mongo\TransactionLog', array('insertTransaction'), array(), '', false, true);
        $mockTransactionLog->expects($this->once())
            ->method('insertTransaction')
            ->will($this->returnValue($mockInsert));

        /* @var $mockTransactionLog \Tripod\Mongo\TransactionLog */
        try {
            $mockTransactionLog->createNewTransaction('transaction_1', array(), array(), 'mydb', 'mycollection');
            $this->fail("Exception should have been thrown by createNewTransaction");
        } catch ( Exception $e){
            $this->assertContains('Error creating new transaction:', $e->getMessage());
        }
    }
}