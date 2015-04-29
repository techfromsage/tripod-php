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
class MongoTripodQueueOperationsDeadTestRefactor
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
    private $tripodQueue=null;

    protected function setUp()
    {
        // this test suite uses a different config that is better suited for testing the scenarios we need to
        date_default_timezone_set('Europe/London');
        $configFileName = dirname(__FILE__).'/data/configQueueOperations.json';

        $config = json_decode(file_get_contents($configFileName), true);
        MongoTripodConfig::setConfig($config);

        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";

        // make sure log statements don't go to stdout during tests...
        $log = new \Monolog\Logger("unittest");
        $log->pushHandler(new \Monolog\Handler\NullHandler());
        MongoTripodBase::$logger = $log;


        $this->tripodTransactionLog = new MongoTransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripodQueue = new MongoTripodQueue();
        $this->tripodQueue->purgeQueue();

        $this->tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops',array('defaultContext'=>'http://talisaspire.com/','async'=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));
        $this->getTripodCollection($this->tripod)->drop();
        $config = MongoTripodConfig::getInstance();

        foreach($config->getCollectionsForViews($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop;
        }
        foreach($config->getCollectionsForSearch($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop;
        }
        foreach($config->getCollectionsForTables($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop;
        }

        $this->loadBaseSearchDataViaTripod();

        $this->tripod->getTripodViews()->generateViewsForResourcesOfType("bibo:Book");
        $this->tripod->getTripodTables()->generateTableRowsForType("bibo:Book");
        // index all the documents
        $cursor = $this->getTripodCollection($this->tripod)->find(array("rdf:type.u"=>array('$in'=>array("bibo:Book","foaf:Person"))),array('_id'=>1));//->limit(20);
        foreach($cursor as $result)
        {
            $this->tripod->getSearchIndexer()->generateAndIndexSearchDocuments($result['_id']['r'], $result['_id']['c'], $this->tripod->getPodName());
        }
    }

    /**
     * Saving a change to a single resource that does not impact any other resources should result in just a single item being added to the queue.
     * todo: new test in composite that this change does not result in any ModifiedSubjects
     */
    public function xtestSingleItemIsAddedToQueueForChangeToSingleSubject()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));
        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple("http://talisaspire.com/resources/doc1", $g2->qname_to_uri("dct:subject"),"astrophysics");

        $tripod->saveChanges($g1, $g2);

        $this->assertEquals(1, $this->tripodQueue->count(), "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();

        $cs = new ChangeSet();
        $cs->from_json($queuedItemData['changeSet']);

        $this->assertEquals(array("http://talisaspire.com/resources/doc1"),$cs->get_subjects_of_change(), "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS, $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertContains(OP_TABLES, $queuedItemData['operations'], "Operations should contain table gen");
        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");
    }

    /**
     * Saving a change to a single resource that does not impact any other resources should result in just a single item being added to the queue.
     * todo: new test in composite that this change does not result in any ModifiedSubjects
     */
    public function xtestSingleItemWithViewsOpIsAddedToQueueForChangeToSingleSubject()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>false, OP_SEARCH=>false)));
        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple("http://talisaspire.com/resources/doc1", $g2->qname_to_uri("dct:subject"),"astrophysics");

        $tripod->saveChanges($g1, $g2);

        $this->assertEquals(1, $this->tripodQueue->count(), "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();

        $cs = new ChangeSet();
        $cs->from_json($queuedItemData['changeSet']);

        $this->assertEquals(array("http://talisaspire.com/resources/doc1"),$cs->get_subjects_of_change(), "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS, $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertFalse(in_array(OP_TABLES, $queuedItemData['operations']), "Operations should not contain table gen");
        $this->assertFalse(in_array(OP_SEARCH, $queuedItemData['operations']), "Operations should not contain search gen");
    }

    public function xtestNoItemIsAddedToQueueForChangeToSingleSubjectWithNoAsyncOps()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));
        $g1 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2 = $tripod->describeResource("http://talisaspire.com/resources/doc1");
        $g2->add_literal_triple("http://talisaspire.com/resources/doc1", $g2->qname_to_uri("dct:subject"),"astrophysics");

        $tripod->saveChanges($g1, $g2);

        $this->assertEquals(0, $this->tripodQueue->count(), "There should be 0 items on the queue");
    }

    /**
     * Saving a change to an entity that appears in the impact index for view/table_rows/search docs of 3 other entities should result in
     * 4 items being placed on the queue, with the operations for each relevant to the configured operations based on the specifications
     * todo: new test in composite for one subject that impacts another
     */
    public function xtestSeveralItemsAddedToQueueForChangeToSeveralSubjects()
    {
        $this->tripod->getTripodTables()->generateTableRowsForType("bibo:Book");

        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));
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

        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");

        $expectedSubjects = array("http://talisaspire.com/resources/doc1","http://talisaspire.com/resources/doc2","http://talisaspire.com/resources/doc3");

        for($i=0; $i<$queueCount; $i++){
            $queuedItem      = $this->tripodQueue->fetchNextQueuedItem();

            $cs = new ChangeSet();
            $cs->from_json($queuedItem['changeSet']);

            $this->assertEquals(3,count($cs->get_subjects_of_change()),"Expected 3 subjects");
            $this->assertEmpty(array_diff($cs->get_subjects_of_change(),$expectedSubjects),"Subjects of change not equal to the set of expected subjects");

            // verify the number of operations is the same
            $this->assertEquals(3,count($queuedItem['operations']),"Expected 3 operations");
        }
   }

    /**
     * When adding a new resource that has never been seen before, we should only see one item added to the queue, with all relevant operations for the type(s) associated with it.
     */
    public function xtestItemAddedToQueueForSubjectNeverSeenBefore()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $subjectUri = "http://talisaspire.com/resources/newdoc";
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"),    "bibo:Book");
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"),    "acorn:Resource");
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:title"),   "This is a new resource");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "history");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "philosophy");

        $tripod->saveChanges(new MongoGraph(), $g);
        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();

        $cs = new ChangeSet();
        $cs->from_json($queuedItemData['changeSet']);

        $this->assertEquals(array($subjectUri),$cs->get_subjects_of_change(), "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS,  $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertContains(OP_TABLES, $queuedItemData['operations'], "Operations should contain table gen");
        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");

        // clear the queue
        $this->tripodQueue->purgeQueue();
        // now lets create an author which isnt linked to any resource therefore this should ONLY trigger Search Gen

        // todo: this needs moving into a test closer to the composites...
//        $g = new MongoGraph();
//        $subjectUri = "http://talisaspire.com/authors/newauthor";
//        $g->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"),    "foaf:Person");
//        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("foaf:name"),   "Verbal Kint");
//
//        $tripod->saveChanges(new MongoGraph(), $g);
//        $queueCount = $this->tripodQueue->count();
//        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");
//        /* @var $queuedItem ModifiedSubject */
//        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();
//        $this->assertEquals($subjectUri, $queuedItemData['r'], "Queued Item should be the one we saved changes to");
//        $this->assertNotContains(OP_VIEWS,  $queuedItemData['operations'], "Operations should not contain view gen");
//        $this->assertNotContains(OP_TABLES, $queuedItemData['operations'], "Operations should not contain table gen");
//        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");
    }

    /**
     * When adding a new resource that has never been seen before, we should see NO item added to the queue when the type of the item
     * does not correspond to any type the configured specifications will look for
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestItemNotAddedToQueueForSubjectNeverSeenBeforeThatHasNoApplicableType()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $subjectUri = "http://talisaspire.com/resources/newdoc";
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"),    "acorn:Resource"); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:title"),   "This is a new resource");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "history");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "philosophy");

        $tripod->saveChanges(new MongoGraph(), $g);
        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(0, $queueCount, "There should only be 0 item on the queue");
    }

    /**
     * When adding a new resource that has never been seen before, we should see NO item added to the queue when the type of the item
     * does not correspond to any type the configured specifications will look for
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestItemAddedToQueueForUpdatedSubjectWithApplicableType()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $subjectUri = "http://talisaspire.com/resources/newdoc2";
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"),    "acorn:Resource"); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($subjectUri, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:title"),   "This is a new resource");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "history");
        $g->add_literal_triple($subjectUri,  $g->qname_to_uri("dct:subject"), "philosophy");

        $tripod->saveChanges(new MongoGraph(), $g);
        $queueCount = $this->tripodQueue->count();
        // Same as previous test
        $this->assertEquals(0, $queueCount, "There should be 0 items on the queue");

        $newGraph = $tripod->describeResource($subjectUri);
        $oldGraph = $tripod->describeResource($subjectUri);
        $newGraph->add_resource_triple($subjectUri, $g->qname_to_uri("rdf:type"), "bibo:Book");
        $tripod->saveChanges($oldGraph, $newGraph);
        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");

        $queueItemData = $this->tripodQueue->fetchNextQueuedItem();
        $this->assertEquals($subjectUri, $queueItemData['r']);
        $ops = array(OP_VIEWS, OP_TABLES, OP_SEARCH);
        $this->assertEmpty(array_diff($ops, $queueItemData['operations']));
    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore it should be the only one queued.
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestSavingMultipleNewEntitiesWhereOnlyOneIsApplicable()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $newSubjectUri1 = "http://talisaspire.com/resources/newdoc1";
        $newSubjectUri2 = "http://talisaspire.com/resources/newdoc2";
        $newSubjectUri3 = "http://talisaspire.com/resources/newdoc3";

        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("acorn:Resource")); // there are no specs that are applicable for this type alone
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

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("rdf:type"),    $g->qname_to_uri("acorn:Resource")); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri("dct:creator"), "http://talisaspire.com/authors/1");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:title"),   "This is yeat another new resource");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "art");
        $g->add_literal_triple($newSubjectUri3,  $g->qname_to_uri("dct:subject"), "design");

        $tripod->saveChanges(new MongoGraph(), $g);
        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();
        $this->assertEquals($newSubjectUri2, $queuedItemData['r'], "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS,  $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertContains(OP_TABLES, $queuedItemData['operations'], "Operations should contain table gen");
        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");
    }

    /**
     * With this test we trying to verify an additional subtlety.
     * When you change the type of a resource to become something different it (in this case) will no longer result in
     * a search doc, table row or view being created since it fails the first basic check ( does it have an rdf:type that matches the type of a Spec ).
     * However the subtlety here is that previously the document did have a search doc, table_row and view generated. After changing its type we need to ensure
     * that those documents are deleted. We cannot pre-empt this prior to putting the item onto the queue, there fore it should be added. When the delegate picks the item up, it
     * is responsible for deleting the OLD view/table_row/search doc and then generating a new one. The delete will succeed, but generation will not produce anything because the
     * new document is not of a compatible type.  As you'll see in the test to confirm this we instantiate each delegate and ask it to process the queued item.
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestChangingResourceTypeQueuesOneItemWillDeleteTheOldViewsTablesAndSearchDocs()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        $subjectUri = "http://talisaspire.com/resources/doc1";

        // before we do anything assert that there is a view, table_row and search doc already generated for this resource
        $view = $this->findOneView($tripod, array('_id.r'=>$subjectUri));
        $tableRow = $this->findOneTableRow($tripod, array('_id.r'=>$subjectUri));
        $searchDoc = $this->findOneSearchDocument($tripod, array('_id.r'=>$subjectUri));
        $this->assertEquals($view['_id']['r'], $subjectUri);
        $this->assertEquals($tableRow['_id']['r'], $subjectUri);
        $this->assertEquals($searchDoc['_id']['r'], $subjectUri);

        // now lets change the type of the resource
        $g1 = $tripod->describeResource($subjectUri);
        $g2 = $tripod->describeResource($subjectUri);
        $g2->remove_resource_triple($subjectUri, $g2->qname_to_uri("rdf:type"), $g2->qname_to_uri("bibo:Book"));
        $tripod->saveChanges($g1, $g2);

        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();
        $this->assertEquals($subjectUri, $queuedItemData['r'], "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS,  $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertContains(OP_TABLES, $queuedItemData['operations'], "Operations should contain table gen");
        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");

        // because we told tripod to process these operations asynchronously we need to explicitly call each delegate to
        // process the given item in this test
        $tripod->getTripodViews()->update($queuedItem);
        $tripod->getTripodTables()->update($queuedItem);
        $tripod->getSearchIndexer()->update($queuedItem);

        // the result should be that the view, table_row and search document has been deleted, so retrieve each
        // and confirm that each is NULL i.e. does not exist in the db anymore
        $view = $this->findOneView($tripod, array('_id.r'=>$subjectUri));
        $tableRow = $this->findOneTableRow($tripod, array('_id.r'=>$subjectUri));
        $searchDoc = $this->findOneSearchDocument($tripod, array('_id.r'=>$subjectUri));
        $this->assertNull($view);
        $this->assertNull($tableRow);
        $this->assertNull($searchDoc);
    }

    /**
     * More complex scenario similar to the one above. Here we change the type of a resource such that it should no longer be applicable based on any specifications.
     * However the resource being changed impacts several others which will need to have their views/tables/search docs regenerated. The outcome we are looking for is
     * a) Prior to the saving the change each resource had  relevant views, tables and search docs already generated
     * b) The resource and the resources it impacts (three of them) are added to the queue with the correct operations specified for each (four in total).
     * c) After each queued item is processed, the changed resource no long has a search document because it is no longer applicable. It's impacted resources still have
     *    views/tables/search docs
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestChangingTypeOfResourceThatImpactsOthers()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        $subjectUri = "http://talisaspire.com/authors/1";
        $expectedQueuedItems = array(
            "http://talisaspire.com/authors/1" => array(
                "operations"=> array(OP_SEARCH) // foaf:Person only has a search doc spec associated with it in config
            ),
            "http://talisaspire.com/resources/doc1" => array(
                "operations"=> array(OP_VIEWS, OP_TABLES, OP_SEARCH)
            ),
            "http://talisaspire.com/resources/doc2" => array(
                "operations"=> array(OP_VIEWS, OP_TABLES, OP_SEARCH)
            ),
            "http://talisaspire.com/resources/doc3" => array(
                "operations"=> array(OP_VIEWS, OP_TABLES, OP_SEARCH)
            ),
        );

        // first confirm that there is a search doc for the author we are changing, and views, tables and search docs for the others
        foreach($expectedQueuedItems as $id=>$ops){
            foreach($ops['operations'] as $op){
                switch($op){
                    case OP_SEARCH:
                        echo "Asserting Search Doc exists for $id\n";
                        $s = $this->findOneSearchDocument($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($s, "Search doc for $id should exist");
                        break;
                    case OP_VIEWS:
                        echo "Asserting View exists for $id\n";
                        $v = $this->findOneView($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($v, "View for $id should exist");
                        break;
                    case OP_TABLES:
                        echo "Asserting Table row exists for $id\n";
                        $t = $this->findOneTableRow($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($t, "Table row doc for $id should exist");
                        break;
                }
            }
        }

        $g1 = $tripod->describeResource($subjectUri);
        $g2 = $tripod->describeResource($subjectUri);

        // change the type and save
        $g2->remove_resource_triple("http://talisaspire.com/authors/1", $g2->qname_to_uri("rdf:type"), $g2->qname_to_uri("foaf:Person") );
        $g2->add_resource_triple("http://talisaspire.com/authors/1", $g2->qname_to_uri("rdf:type"), $g2->qname_to_uri("foaf:Organization") );
        $tripod->saveChanges($g1, $g2);

        // there should be 4 items on the queue
        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(4, $queueCount, "There should only be 4 items on the queue");

        for($i=0; $i<$queueCount; $i++){
            $queuedItemData      = $this->tripodQueue->fetchNextQueuedItem();
            $queuedItemRId   = $queuedItemData[_ID_RESOURCE];
            if(!array_key_exists( $queuedItemRId, $expectedQueuedItems)){
                $this->fail("Queued Item was not found in the set of Expected Items");
            }

            // verify the number of operations is the same
            $queuedItemOpsCount   = count($queuedItemData['operations']);
            $queuedItemData['operations'];
            $expectedItemOpsCount = count($expectedQueuedItems[$queuedItemRId]["operations"]);
            $this->assertEquals($expectedItemOpsCount, $queuedItemOpsCount, "Queued Item does not contain the expected number of operations");
            // assert queued item operations appear in the expected item operations
            foreach($queuedItemData['operations'] as $op){
                $this->assertContains($op, $expectedQueuedItems[$queuedItemRId]["operations"], "$op was not found in set of Expected Operations for this queued item");
                // now execute the operation
//                switch($op){
//                    case OP_SEARCH:
//                        echo "Executing Search Operation for $id\n";
//                        $tripod->getSearchIndexer()->update($queuedItem);;
//                        break;
//                    case OP_VIEWS:
//                        echo "Executing View Operation for $id\n";
//                        $tripod->getTripodViews()->update($queuedItem);
//                        break;
//                    case OP_TABLES:
//                        echo "Executing Table Row Operation for $id\n";
//                        $tripod->getTripodTables()->update($queuedItem);
//                        break;
//                }
            }
        }

        // by this point the queued items are asserted and each item has been processed
        // now assert that the search doc for the author that we changed has been deleted
        // Note, *only* i_search_author should have been affected by the changed graph, there should still be a
        // matching search document for i_search_resource
        $s = $this->findOneSearchDocument($tripod, array('_id.r'=>$id, '_id.type'=>"i_search_author"));
        $this->assertNull($s);

        $s = $this->findOneSearchDocument($tripod, array('_id.r'=>$id, '_id.type'=>"i_search_resource"));
        $this->assertNotNull($s);
        // remove it from the expectedQueuedItems structure so we can verify the others
        unset($expectedQueuedItems[$subjectUri]);

        // assert that the views/tables/and search docs for the other entities have not been deleted
        foreach($expectedQueuedItems as $id=>$ops){
            foreach($ops['operations'] as $op){
                switch($op){
                    case OP_SEARCH:
                        echo "Asserting Search Doc exists for $id\n";
                        $s = $this->findOneSearchDocument($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($s, "Search doc for $id should exist");
                        break;
                    case OP_VIEWS:
                        echo "Asserting View exists for $id\n";
                        $v = $this->findOneView($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($v, "View for $id should exist");
                        break;
                    case OP_TABLES:
                        echo "Asserting Table row exists for $id\n";
                        $t = $this->findOneTableRow($tripod, array('_id.r'=>$id));
                        $this->assertNotNull($t, "Table row doc for $id should exist");
                        break;
                }
            }
        }
    }


    /**
     * Delete a single resource that does not impact any others.
     * Assumes that views/tables/search docs exist for the entity before the change
     * Verifies that a single queued item is added to the queue, with the correct set of operations
     * After processing the queued item there should be no view/table/search doc for this resource
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestDeleteSingleResourceWithNoImpactQueuesSingleItem()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        $subjectUri = "http://talisaspire.com/resources/doc1";

        // before we do anything assert that there is a view, table_row and search doc already generated for this resource
        $view = $this->findOneView($tripod, array('_id.r'=>$subjectUri));
        $tableRow = $this->findOneTableRow($tripod, array('_id.r'=>$subjectUri));
        $searchDoc = $this->findOneSearchDocument($tripod, array('_id.r'=>$subjectUri));
        $this->assertEquals($view['_id']['r'], $subjectUri);
        $this->assertEquals($tableRow['_id']['r'], $subjectUri);
        $this->assertEquals($searchDoc['_id']['r'], $subjectUri);

        // now lets change the type of the resource
        $g1 = $tripod->describeResource($subjectUri);
        $g2 = new MongoGraph();
        $tripod->saveChanges($g1, $g2);

        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(1, $queueCount, "There should only be 1 item on the queue");
        /* @var $queuedItem ModifiedSubject */
        $queuedItemData = $this->tripodQueue->fetchNextQueuedItem();
        $this->assertEquals(array($subjectUri), array_keys($queuedItemData['subjectsAndPredicatesOfChange']), "Queued Item should be the one we saved changes to");
        $this->assertContains(OP_VIEWS,  $queuedItemData['operations'], "Operations should contain view gen");
        $this->assertContains(OP_TABLES, $queuedItemData['operations'], "Operations should contain table gen");
        $this->assertContains(OP_SEARCH, $queuedItemData['operations'], "Operations should contain search gen");


        $this->tripodQueue->processNext();
//        // because we told tripod to process these operations asynchronously we need to explicitly call each delegate to
//        // process the given item in this test
//        $tripod->getTripodViews()->update($queuedItem);
//        $tripod->getTripodTables()->update($queuedItem);
//        $tripod->getSearchIndexer()->update($queuedItem);

        // the result should be that the view, table_row and search document has been deleted, so retrieve each
        // and confirm that each is NULL i.e. does not exist in the db anymore
        $view = $this->findOneView($tripod, array('_id.r'=>$subjectUri));
        $tableRow = $this->findOneTableRow($tripod, array('_id.r'=>$subjectUri));
        $searchDoc = $this->findOneSearchDocument($tripod, array('_id.r'=>$subjectUri));
        $this->assertNull($view);
        $this->assertNull($tableRow);
        $this->assertNull($searchDoc);
    }


    /**
     * Delete a single resource that does not impact any others, and has no views/tables/search docs generated
     *
     * Verifies that nothing is added to the queue the resource is no longer applicable, and because there are no
     * views/tables/searches generated for it already, there are no impacted documents to regenerate.
     * todo: move this test to a more appropriate composites type test
     */
    public function xtestDeleteSingleResourceWithNoImpactButNoExistingViewsTablesSearchDocsDoesntQueueAnything()
    {
        // create a tripod instance that will send all operations to the queue
        $tripod = new MongoTripod('CBD_testing','tripod_php_testing_queue_ops', array('defaultContext'=>'http://talisaspire.com/', 'async'=>array(OP_VIEWS=>true, OP_TABLES=>true, OP_SEARCH=>true)));

        $subjectUri = "http://talisaspire.com/resources/doc1";

        // before we do anything remove any existing views/tables/search docs for this resource
        $this->removeFromViews($tripod, array('_id.r'=>$subjectUri));
        $this->removeFromTableRows($tripod, array('_id.r'=>$subjectUri));
        $this->removeFromSearchDocuments($tripod, array('_id.r'=>$subjectUri));

        // now lets change the type of the resource
        $g1 = $tripod->describeResource($subjectUri);
        $g2 = new MongoGraph();
        $tripod->saveChanges($g1, $g2);

        $queueCount = $this->tripodQueue->count();
        $this->assertEquals(0, $queueCount, "There should only be 0 item on the queue");
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     * @return array|null
     */
    protected function findOneView(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForViews($tripod->getStoreName()) as $c)
        {
            $view = $c->findOne($query);
            if($view)
            {
               return $view;
            }
        }
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     * @return array|null
     */
    protected function findOneTableRow(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForTables($tripod->getStoreName()) as $c)
        {
            $tableRow = $c->findOne($query);
            if($tableRow)
            {
                return $tableRow;
            }
        }
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     * @return array|null
     */
    protected function findOneSearchDocument(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForSearch($tripod->getStoreName()) as $c)
        {
            $searchDoc = $c->findOne($query);
            if($searchDoc)
            {
                return $searchDoc;
            }
        }
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     */
    protected function removeFromViews(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForViews($tripod->getStoreName()) as $c)
        {
            $c->remove($query);
        }
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     */
    protected function removeFromTableRows(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForTables($tripod->getStoreName()) as $c)
        {
            $c->remove($query);
        }
    }

    /**
     * @param MongoTripod $tripod
     * @param array $query
     */
    protected function removeFromSearchDocuments(MongoTripod $tripod, array $query)
    {
        foreach(MongoTripodConfig::getInstance()->getCollectionsForSearch($tripod->getStoreName()) as $c)
        {
            $c->remove($query);
        }
    }
}