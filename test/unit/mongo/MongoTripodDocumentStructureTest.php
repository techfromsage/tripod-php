<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripod.class.php';

class MongoTripodDocumentStructureTest extends MongoTripodTestBase
{
    /**
     * @var MongoTripod|PHPUnit_Framework_MockObject_MockObject
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function setUp()
    {
        parent::setup();
        //Mongo::setPoolSize(200);

        $this->tripodTransactionLog = new MongoTransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        // Stub ouf 'addToElastic' search to prevent writes into Elastic Search happening by default.
        $this->tripod = $this->getMock('MongoTripod', array('addToSearchIndexQueue'), array('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/')));
        $this->tripod->expects($this->any())->method('addToSearchIndexQueue');

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->loadBaseDataViaTripod();
    }

    public function testDocumentContainsDefaultProperties()
    {
        $id = array("r"=>"http://talisaspire.com/resources/testDocument", "c"=>"http://talisaspire.com/");

        $graph = new MongoGraph();
        $graph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'TEST TITLE');
        $this->tripod->saveChanges(new MongoGraph(), $graph);

        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 0);
        $this->assertDocumentHasProperty($id, _UPDATED_TS);
        $this->assertDocumentHasProperty($id, _CREATED_TS);
    }

    public function testDocumentTimeStampsAreUpdatedCorrectlyAfterMultipleWritesAndDelete()
    {
        // create an initial document
        $id = array("r"=>"http://talisaspire.com/resources/testDocument", "c"=>"http://talisaspire.com/");

        $graph = new MongoGraph();
        $graph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'TEST TITLE');
        $this->tripod->saveChanges(new MongoGraph(), $graph);
        // assert that it is at version 0
        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 0);

        // retrieve the document from mongo ( rather than the graph ) capture the updated and created ts
        $document = $this->getDocument($id);
        $_updated_ts = $document[_UPDATED_TS];
        $_created_ts = $document[_CREATED_TS];

        sleep(1); // have to sleep to make sure ->sec will be greater between writes.

        // change document through tripod
        $newGraph = new MongoGraph();
        $newGraph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'CHANGED TITLE');
        $this->tripod->saveChanges($graph, $newGraph);

        // assert that it is at version 1
        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 1);

        // assert that the $_updated_ts has changed, but the created_ts is the same
        $updated_document = $this->getDocument($id);
        $this->assertEquals($_created_ts, $updated_document[_CREATED_TS]);
        $this->assertNotEquals($_updated_ts, $updated_document[_UPDATED_TS]);
        // assert that the seconds for the updated document _updated_ts is greated than the first version
        $this->assertGreaterThan($_updated_ts->sec, $updated_document[_UPDATED_TS]->sec);

        sleep(1);

        // update again
        $finalGraph = new MongoGraph();
        $finalGraph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'CHANGED TITLE AGAIN');
        $this->tripod->saveChanges($newGraph, $finalGraph);

       // assert that it is at version 2
        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 2);

        // assert that the $_updated_ts has changed, but the created_ts is the same
        $final_document = $this->getDocument($id);
        $this->assertEquals($updated_document[_CREATED_TS], $final_document[_CREATED_TS]);
        $this->assertNotEquals($updated_document[_UPDATED_TS], $final_document[_UPDATED_TS]);
        $this->assertGreaterThan($updated_document[_UPDATED_TS]->sec, $final_document[_UPDATED_TS]->sec);

        sleep(1);

        // now delete through tripod, only the _ID, _VERSION, _UPDATED_TS and _CREATED_TS properties should exist on the document
        // updated ts will have changed the created should not have
        $this->tripod->saveChanges($finalGraph, new MongoGraph());

        $this->assertDocumentExists($id);
        $deleted_document = $this->getDocument($id);
        $this->assertDocumentHasProperty($id, _VERSION);
        $this->assertDocumentHasProperty($id, _UPDATED_TS);
        $this->assertDocumentHasProperty($id, _CREATED_TS);
        $this->assertDocumentDoesNotHaveProperty($id, 'searchterms:title');

        $this->assertEquals($final_document[_CREATED_TS], $deleted_document[_CREATED_TS]);
        $this->assertNotEquals($final_document[_UPDATED_TS], $deleted_document[_UPDATED_TS]);
        $this->assertGreaterThan($final_document[_UPDATED_TS]->sec, $deleted_document[_UPDATED_TS]->sec);
    }


    /**
     * This test verifies that if a document was previously added to mongo without any timestamps i.e. _UPDATED_TS and _CREATED_TS
     * then on a tripod write only the _UPDATED_TS will be added to the document
     */
    public function testOnlyDocumentUpdatedTimestampIsAddedToDocumentThatDidntHaveTimestampsToBeginWith()
    {
        // add the initial document, but not through Tripod!
        $_id = array("r"=>"http://talisaspire.com/resources/testDocument2","c"=>"http://talisaspire.com/");
        $document = array(
            '_id' => $_id,
            'dct:title' => array('l'=>'some title'),
            '_version' => 0
        );

        // verify initial document before we proceed, should have the triple we added, and a _version but not a
        // _UPDATED_TS or a _CREATED_TS
        $this->addDocument($document);
        $this->assertDocumentExists($_id);
        $this->assertDocumentHasProperty($_id, _VERSION, 0);
        $this->assertDocumentHasProperty($_id, "dct:title", array("l"=>"some title"));
        $this->assertDocumentDoesNotHaveProperty($_id, _UPDATED_TS);
        $this->assertDocumentDoesNotHaveProperty($_id, _CREATED_TS);

        // change the document through tripod, for this im just doing a new addition
        $graph = new MongoGraph();
        $graph->add_literal_triple($_id["r"], $graph->qname_to_uri('searchterms:title'), 'a new property');
        $this->tripod->saveChanges(new MongoGraph(), $graph);

        // Now assert, document should contain the additiona triple we added, an updated _version.
        // Should now also contain an _UPDATED_TS but not a _CREATED_TS
        $this->assertDocumentExists($_id);
        $this->assertDocumentHasProperty($_id, _VERSION, 1);
        $this->assertDocumentHasProperty($_id, _UPDATED_TS);
        $this->assertDocumentHasProperty($_id, "dct:title", array("l"=>"some title"));
        $this->assertDocumentHasProperty($_id, "searchterms:title", array("l"=>"a new property"));
        $this->assertDocumentDoesNotHaveProperty($_id, _CREATED_TS);
    }
}
