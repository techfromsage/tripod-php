<?php
require_once dirname(__FILE__).'/../TripodTestBase.php';
/** @noinspection PhpIncludeInspection */

/**
 * This test suite was added to specifically verify behaviour of code
 * during MongoTripod->storeChanges.
 * namely that documents are locked, transactions created, documents unlocked etc.
 *
 * Class MongoTripodTransactionRollbackTest
 */
class MongoTripodTransactionRollbackTest extends TripodTestBase
{
    /**
     * @var MongoTripod
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransactionLog = null;

    protected $labeller = null;

    protected function setUp()
    {
        parent::setup();

        $type = MongoTripodConfig::getInstance()->getTransactionLogType();
        $this->tripodTransactionLog = new $type();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->labeller = new MongoTripodLabeller();

        // Stub out 'addToElastic' search to prevent writes into Elastic Search happening by default.
        $tripod = $this->getMock('MongoTripod', array('addToSearchIndexQueue'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $tripod->expects($this->any())->method('addToSearchIndexQueue');

        /** @var $tripod MongoTripod */
        $tripod->collection->drop();

        $tripod->lCollection->drop();

        $tripod->setTransactionLog($this->tripodTransactionLog);

        $this->tripod = $tripod;
    }

    public function testTransactionRollbackDuringLockAllDocuments()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> new MongoDate(strtotime("2013-03-21 00:00:00")),
            '_uts'=> new MongoDate(strtotime("2013-03-21 00:00:00"))
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> new MongoDate(strtotime("2013-03-21 00:00:00")),
            '_uts'=> new MongoDate(strtotime("2013-03-21 00:00:00"))
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new MongoGraph();
        $nG->add_graph($g1);
        $nG->add_graph($g2);
        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('MongoTripod', array('generateTransactionId','lockSingleDocument'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripod->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripod->expects($this->exactly(2*20)) //20 retries for 2 subjects
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCauseFailureCallback')));

        /** @var $mockTripod MongoTripod */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('TripodException should have been thrown');
        }
        catch (TripodException $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $this->tripod->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("Did not obtain locks on documents", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);

    }

    public function testTransactionRollbackDuringLockAllDocumentsWithEmptyOriginalCBDS()
    {
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';

        $oG = new MongoGraph();
        $nG = new MongoGraph();
        // save two completely new entities
        $nG->add_resource_triple($subjectOne, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Resource"));
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title two");
        $nG->add_resource_triple($subjectTwo, $nG->qname_to_uri("rdf:type"), $nG->qname_to_uri("acorn:Book"));
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title four");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('MongoTripod', array('generateTransactionId','lockSingleDocument'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripod->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripod->expects($this->exactly(2*20)) //20 retries for 2 subjects
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCauseFailureCallback')));

        /** @var $mockTripod MongoTripod */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('TripodException should have been thrown');
        }
        catch (TripodException $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->is_empty());

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $this->tripod->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("Did not obtain locks on documents", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);
    }


    public function testTransactionRollbackDuringCreateTransaction()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> new MongoDate(strtotime("2013-03-21 00:00:00")),
            '_uts'=> new MongoDate(strtotime("2013-03-21 00:00:00"))
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> new MongoDate(strtotime("2013-03-21 00:00:00")),
            '_uts'=> new MongoDate(strtotime("2013-03-21 00:00:00"))
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new MongoGraph();
        $nG->add_graph($g1);
        $nG->add_graph($g2);
        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        // some values we want explicitly returned from mocks
        $mockExpectedException = new TripodException('Error creating new transaction');
        $mockTransactionId = 'transaction_1';

        $mockTransactionLog = $this->getMock('MongoTransactionLog', array('createNewTransaction', 'cancelTransaction','failTransaction'), array(),'',false, false);
        $mockTransactionLog->expects($this->once())
            ->method('createNewTransaction')
            ->will($this->throwException($mockExpectedException));
        $mockTransactionLog->expects(($this->once()))
            ->method('cancelTransaction')
            ->with($this->equalTo($mockTransactionId), $this->equalTo($mockExpectedException));
        $mockTransactionLog->expects(($this->once()))
            ->method('failTransaction')
            ->with($this->equalTo($mockTransactionId));

        $mockTripod = $this->getMock('MongoTripod', array('generateTransactionId','lockSingleDocument','getTransactionLog'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripod->expects($this->once())
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripod->expects($this->exactly(2))
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCallback')));
        $mockTripod->expects($this->exactly(3))
            ->method('getTransactionLog')
            ->will($this->returnValue($mockTransactionLog));

        try
        {
            /* @var $mockTripod MongoTripod */
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('TripodException should have been thrown');
        }
        catch (TripodException $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        // make sure the documents are not polluted with locks
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
    }

    public function testTransactionRollbackDuringApplyChanges()
    {
        // Save some basic data into the db before we create a transaction to modify it
        $subjectOne = 'http://example.com/resources/1';
        $subjectTwo = 'http://example.com/resources/2';
        $doc1 = array(
            '_id'=>array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Resource'),
            'dct:title'=>array(array('l'=>'Title one'),array('l'=>'Title two')),
            '_version'=>0,
            '_cts'=> new MongoDate(),
            '_uts'=> new MongoDate()
        );

        $doc2 = array(
            '_id'=>array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'),
            'rdf:type'=>array('u'=>'acorn:Book'),
            'dct:title'=>array(array('l'=>'Title three'),array('l'=>'Title four')),
            '_version'=>0,
            '_cts'=> new MongoDate(),
            '_uts'=> new MongoDate()
        );
        $this->addDocument($doc1);
        $this->addDocument($doc2);

        // now lets modify the data using tripod
        $g1 = $this->tripod->describeResources(array($subjectOne),'http://talisaspire.com/');
        $g2 = $this->tripod->describeResources(array($subjectTwo),'http://talisaspire.com/');

        $oG = new MongoGraph();
        $oG->add_graph($g1);
        $oG->add_graph($g2);

        $nG = new MongoGraph();
        $nG->add_graph($oG);

        $nG->remove_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Title one");
        $nG->add_literal_triple($subjectOne, $nG->qname_to_uri("dct:title"), "Updated Title one");
        $nG->remove_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Title three");
        $nG->add_literal_triple($subjectTwo, $nG->qname_to_uri("dct:title"), "Updated Title three");

        $mockTransactionId = 'transaction_1';
        $mockTripod = $this->getMock('MongoTripod', array('generateTransactionId','lockSingleDocument','applyChangeSet'), array('CBD_testing','testing',array('defaultContext'=>'http://talisaspire.com/')));
        $mockTripod->expects($this->exactly(1))
            ->method('generateTransactionId')
            ->will($this->returnValue($mockTransactionId));
        $mockTripod->expects($this->exactly(2))
            ->method('lockSingleDocument')
            ->will($this->returnCallback(array($this, 'lockSingleDocumentCallback')));
        $mockTripod->expects($this->once())->method('applyChangeSet')->will($this->throwException(new Exception("TripodException throw by mock test during applychangeset")));
        /** @var $mockTripod MongoTripod */
        $mockTripod->setTransactionLog($this->tripodTransactionLog);

        try
        {
            $mockTripod->saveChanges($oG, $nG,"http://talisaspire.com/");
            $this->fail('TripodException should have been thrown');
        }
        catch (TripodException $e)
        {
            // Squash the exception here as we need to continue running the assertions.
        }

        // make sure the subjects werent changed
        $uG = $this->tripod->describeResources(array($subjectOne, $subjectTwo));
        $this->assertTrue($uG->has_resource_triple($subjectOne, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Resource")));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title one'));
        $this->assertTrue($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Title two'));
        $this->assertFalse($uG->has_literal_triple($subjectOne, $uG->qname_to_uri("dct:title"), 'Updated Title two'));
        $this->assertTrue($uG->has_resource_triple($subjectTwo, $uG->qname_to_uri("rdf:type"), $uG->qname_to_uri("acorn:Book")));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title three'));
        $this->assertTrue($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Title four'));
        $this->assertFalse($uG->has_literal_triple($subjectTwo, $uG->qname_to_uri("dct:title"), 'Updated Title three'));

        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectOne, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS, $this->tripod);
        $this->assertDocumentDoesNotHaveProperty(array('r'=>$subjectTwo, 'c'=>'http://talisaspire.com/'), _LOCKED_FOR_TRANS_TS, $this->tripod);

        $transaction = $this->tripod->getTransactionLog()->getTransaction($mockTransactionId);
        $this->assertNotNull($transaction);
        $this->assertEquals("TripodException throw by mock test during applychangeset", $transaction['error']['reason']);
        $this->assertEquals("failed", $transaction['status']);

    }


    /* HELPER FUNCTIONS BELOW HERE */

    /**
     * This helper function is a callback that assumes that the document being changed does not already exist in the db
     * Depending on the values passed it either mimics the behaviour of the real lockSingleDocument method on MongoTripod, or it
     * returns an empty array. Have to do this because I want to mock the behavour of lockSingleDocument so I can throw an error for one subject
     * but allow it go through normally for another which you cant do with a mock, hence this hack!
     * @param $s
     * @param $transactionId
     * @param $context
     * @return array
     */
    public function lockSingleDocumentCauseFailureCallback($s, $transactionId, $context)
    {
        if($s=='http://example.com/resources/1')
        {
            return $this->lockSingleDocumentCallback($s, $transactionId, $context);
        } else {
            return array();
        }
    }

    /**
     * This is a private method that performs exactly the same operation as MongoTripod::lockSingleDocument, the reason this is duplicated here
     * is so that we can simulate the correct locking of documents as part of mocking a workflow that will lock a document correctly but not another
     * @param $s
     * @param $transaction_id
     * @param $contextAlias
     * @return array
     */
    public function lockSingleDocumentCallback($s, $transaction_id, $contextAlias)
    {
        $lCollection = $this->tripod->db->selectCollection(LOCKS_COLLECTION);
        $countEntriesInLocksCollection = $lCollection->count(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));

        if($countEntriesInLocksCollection > 0) //Subject is already locked
            return false;
        else{
            try{ //Add a entry to locks collection for this subject, will throws exception if an entry already there
                $result = $lCollection->insert(
                    array(
                        '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias),
                        _LOCKED_FOR_TRANS => $transaction_id,
                        _LOCKED_FOR_TRANS_TS=>new MongoDate()
                    ),
                    array("w" => 1)
                );

                if(!$result["ok"] || $result['err']!=NULL){
                    throw new Exception("Failed to lock document with error message- " . $result['err']);
                }
            }
            catch(Exception $e) { //Subject is already locked or unable to lock
                $this->debugLog(MONGO_LOCK,
                    array(
                        'description'=>'MongoTripod::lockSingleDocument - failed with exception',
                        'transaction_id'=>$transaction_id,
                        'subject'=>$s,
                        'exception-message' => $e->getMessage()
                    )
                );
                return false;
            }

            //Let's get original document for processing.
            $document  = $this->tripod->collection->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
            if(empty($document)){ //if document is not there, create it
                try{
                    $result = $this->tripod->collection->insert(
                        array(
                            '_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)
                        ),
                        array("w" => 1)
                    );

                    if(!$result["ok"] || $result['err']!=NULL){
                        throw new Exception("Failed to create new document with error message- " . $result['err']);
                    }
                    $document  = $this->tripod->collection->findOne(array('_id' => array(_ID_RESOURCE => $this->labeller->uri_to_alias($s), _ID_CONTEXT => $contextAlias)));
                }
                catch(Exception $e){
                    $this->errorLog(MONGO_LOCK,
                        array(
                            'description'=>'MongoTripod::lockSingleDocument - failed when creating new document',
                            'transaction_id'=>$transaction_id,
                            'subject'=>$s,
                            'exception-message' => $e->getMessage()
                        )
                    );
                    return false;
                }
            }
            return $document;
        }
    }
}

