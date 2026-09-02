<?php

declare(strict_types=1);

use MongoDB\BSON\UTCDateTime;
use Tripod\Mongo\Driver;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TransactionLog;

class MongoTripodDocumentStructureTest extends MongoTripodTestBase
{
    protected function setUp(): void
    {
        parent::setup();

        $this->tripodTransactionLog = new TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing', [
            'defaultContext' => 'http://talisaspire.com/',
        ]);

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->loadResourceDataViaTripod();
    }

    public function testDocumentContainsDefaultProperties(): void
    {
        $id = ['r' => 'http://talisaspire.com/resources/testDocument', 'c' => 'http://talisaspire.com/'];

        $graph = new MongoGraph();
        $graph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges(new MongoGraph(), $graph);

        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 0);
        $this->assertDocumentHasProperty($id, _UPDATED_TS);
        $this->assertDocumentHasProperty($id, _CREATED_TS);
    }

    public function testDocumentTimeStampsAreUpdatedCorrectlyAfterMultipleWritesAndDelete(): void
    {
        // create an initial document
        $id = ['r' => 'http://talisaspire.com/resources/testDocument', 'c' => 'http://talisaspire.com/'];

        $graph = new MongoGraph();
        $graph->add_literal_triple($id['r'], $graph->qname_to_uri('searchterms:title'), 'TEST TITLE');

        $this->tripod->saveChanges(new MongoGraph(), $graph);
        // assert that it is at version 0
        $this->assertDocumentExists($id);
        $this->assertDocumentHasProperty($id, _VERSION, 0);

        // retrieve the document from mongo ( rather than the graph ) capture the updated and created ts
        $document = $this->getDocument($id);
        $this->assertNotNull($document);
        $_updated_ts = $document[_UPDATED_TS];
        $this->assertInstanceOf(UTCDateTime::class, $_updated_ts);
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
        $this->assertNotNull($updated_document);
        $this->assertEquals($_created_ts, $updated_document[_CREATED_TS]);
        $updatedDocUpdatedTs = $updated_document[_UPDATED_TS];
        $this->assertInstanceOf(UTCDateTime::class, $updatedDocUpdatedTs);
        $this->assertNotEquals($_updated_ts->__toString(), $updatedDocUpdatedTs->__toString());
        // assert that the seconds for the updated document _updated_ts is greated than the first version

        $this->assertGreaterThan($_updated_ts->__toString(), $updatedDocUpdatedTs->__toString());

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
        $this->assertNotNull($final_document);
        $this->assertEquals($updated_document[_CREATED_TS], $final_document[_CREATED_TS]);
        $finalDocUpdatedTs = $final_document[_UPDATED_TS];
        $this->assertInstanceOf(UTCDateTime::class, $finalDocUpdatedTs);
        $this->assertNotEquals($updatedDocUpdatedTs->__toString(), $finalDocUpdatedTs->__toString());
        $this->assertGreaterThan($updatedDocUpdatedTs->__toString(), $finalDocUpdatedTs->__toString());

        sleep(1);

        // now delete through tripod, only the _ID, _VERSION, _UPDATED_TS and _CREATED_TS properties should exist on the document
        // updated ts will have changed the created should not have
        $this->tripod->saveChanges($finalGraph, new MongoGraph());

        $this->assertDocumentExists($id);
        $deleted_document = $this->getDocument($id);
        $this->assertNotNull($deleted_document);
        $this->assertDocumentHasProperty($id, _VERSION);
        $this->assertDocumentHasProperty($id, _UPDATED_TS);
        $this->assertDocumentHasProperty($id, _CREATED_TS);
        $this->assertDocumentDoesNotHaveProperty($id, 'searchterms:title');

        $this->assertEquals($final_document[_CREATED_TS], $deleted_document[_CREATED_TS]);
        $deletedDocUpdatedTs = $deleted_document[_UPDATED_TS];
        $this->assertInstanceOf(UTCDateTime::class, $deletedDocUpdatedTs);
        $this->assertNotEquals($finalDocUpdatedTs->__toString(), $deletedDocUpdatedTs->__toString());
        $this->assertGreaterThan($finalDocUpdatedTs->__toString(), $deletedDocUpdatedTs->__toString());
    }

    /**
     * This test verifies that if a document was previously added to mongo without any timestamps i.e. _UPDATED_TS and _CREATED_TS
     * then on a tripod write only the _UPDATED_TS will be added to the document.
     */
    public function testOnlyDocumentUpdatedTimestampIsAddedToDocumentThatDidntHaveTimestampsToBeginWith(): void
    {
        // add the initial document, but not through Driver!
        $_id = ['r' => 'http://talisaspire.com/resources/testDocument2', 'c' => 'http://talisaspire.com/'];
        $document = [
            '_id' => $_id,
            'dct:title' => ['l' => 'some title'],
            '_version' => 0,
        ];

        // verify initial document before we proceed, should have the triple we added, and a _version but not a
        // _UPDATED_TS or a _CREATED_TS
        $this->addDocument($document);
        $this->assertDocumentExists($_id);
        $this->assertDocumentHasProperty($_id, _VERSION, 0);
        $this->assertDocumentHasProperty($_id, 'dct:title', ['l' => 'some title']);
        $this->assertDocumentDoesNotHaveProperty($_id, _UPDATED_TS);
        $this->assertDocumentDoesNotHaveProperty($_id, _CREATED_TS);

        // change the document through tripod, for this im just doing a new addition
        $graph = new MongoGraph();
        $graph->add_literal_triple($_id['r'], $graph->qname_to_uri('searchterms:title'), 'a new property');

        $this->tripod->saveChanges(new MongoGraph(), $graph);

        // Now assert, document should contain the additiona triple we added, an updated _version.
        // Should now also contain an _UPDATED_TS but not a _CREATED_TS
        $this->assertDocumentExists($_id);
        $this->assertDocumentHasProperty($_id, _VERSION, 1);
        $this->assertDocumentHasProperty($_id, _UPDATED_TS);
        $this->assertDocumentHasProperty($_id, 'dct:title', ['l' => 'some title']);
        $this->assertDocumentHasProperty($_id, 'searchterms:title', ['l' => 'a new property']);
        $this->assertDocumentDoesNotHaveProperty($_id, _CREATED_TS);
    }
}
