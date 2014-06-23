<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/delegates/MongoTripodTables.class.php';
require_once 'src/mongo/MongoTripod.class.php';
require_once 'src/mongo/MongoGraph.class.php';


class MongoTripodTablesTest extends MongoTripodTestBase
{
    /**
     * @var MongoTripod
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransationLog = null;

    /**
     * @var MongoTripodTables
     */
    protected $tripodTables = null;

    private $tablesConstParams = null;

    protected function setUp()
    {
        parent::setup();
        //Mongo::setPoolSize(200);

        $this->tripodTransactionLog = new MongoTransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new MongoTripod("CBD_testing", "testing", array("async"=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));

        $this->tripod->collection->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->loadBaseDataViaTripod();

        // purge Queue
        $queue = new MongoTripodQueue();
        $queue->purgeQueue();

        $this->tablesConstParams = array($this->tripod->db,$this->tripod->collection,'http://talisaspire.com/');

        $this->tripodTables = new MongoTripodTables($this->tripod->db,$this->tripod->collection,null); // pass null context, should default to http://talisaspire.com

        // purge tables
        $this->tripodTables->db->selectCollection("table_rows")->drop();
    }

    /**
     * Generate dummy config that we can use for creating a MongoTripodConfig object
     * @access private
     * @return array
     */
    private function generateMongoTripodTestConfig()
    {
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost",
                "collections" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","connStr"=>"mongodb://localhost");
        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "connStr"=>"mongodb://tloghost:27017,tloghost:27018"
        );
        return $config;
    }

    /**
     * Generate table rows based off an id
     * @param string $id
     * @access private
     * @return array
     */
    private function generateTableRows($id)
    {
        $this->tripodTables->generateTableRows($id);
        $rows = $this->tripodTables->getTableRows($id);

        return $rows;
    }

    public function testTripodSaveChangesUpdatesLiteralTripleInTable()
    {
        $this->tripodTables->generateTableRows("t_resource",'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2');

        $t1 = $this->tripodTables->getTableRows("t_resource",array("_id.r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2"));

        $expectedIsbn13s = array("9780393929691","9780393929691-2");
        $this->assertEquals($expectedIsbn13s,$t1['results'][0]['isbn13']);

        $g1 = $this->tripod->describeResource("http://talisaspire.com/works/4d101f63c10a6");
        $g2 = $this->tripod->describeResource("http://talisaspire.com/works/4d101f63c10a6");

        $g2->add_literal_triple("http://talisaspire.com/works/4d101f63c10a6",$g2->qname_to_uri("bibo:isbn13"),"9780393929691-3");
        $this->tripod->saveChanges($g1,$g2,'http://talisaspire.com/');

        $t2 = $this->tripodTables->getTableRows("t_resource",array("_id.r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2"));

        $expectedIsbn13s = array("9780393929691","9780393929691-2","9780393929691-3");
        $this->assertEquals($expectedIsbn13s,$t2['results'][0]['isbn13']);
    }

// TODO: work out if these tests are still relevant necessary now that impacted documents are calculated outside the update() method
//    public function testTripodQueuedWorkTriggersRegenerationOfTwoResources()
//    {
//        $mockTables = $this->getMock('MongoTripodTables', array('generateTableRows'), $this->tablesConstParams);
//        $mockTables->expects($this->at(0))->method('generateTableRows')->with("t_resource","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA","http://talisaspire.com/");
//        $mockTables->expects($this->at(1))->method('generateTableRows')->with("t_resource","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2","http://talisaspire.com/");
//
//        // generate table rows
//        $this->tripodTables->generateTableRows("t_resource");
//
//        $queuedItem = new ModifiedSubject(array("r"=>"http://talisaspire.com/works/4d101f63c10a6","c"=>'http://talisaspire.com/'));
//        // next, trigger regen for work we know is associated with 2x resources. Should trigger view regen for resources
//        $mockTables->update($queuedItem);
//    }
//
//    public function testTripodQueuedWorkTriggersRegenerationOfOneResource()
//    {
//        $mockTables = $this->getMock('MongoTripodTables', array('generateTableRows'), $this->tablesConstParams);
//        $mockTables->expects($this->once())->method('generateTableRows')->with("t_resource","http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2","http://talisaspire.com/");
//
//        // generate table rows
//        $this->tripodTables->generateTableRows("t_resource",'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2');
//
//        $queuedItem = new ModifiedSubject(array("r"=>"http://talisaspire.com/works/4d101f63c10a6","c"=>'http://talisaspire.com/'));
//        // next, trigger regen for work we know is associated with resource above. Should trigger view regen for resource
//        $mockTables->update($queuedItem);
//    }

    public function testGenerateTableRowsWithCounts()
    {
        $this->tripodTables->generateTableRows("t_source_count");

        $t1 = $this->tripodTables->getTableRows("t_source_count");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['source_count']),"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertEquals(0,$result['random_predicate_count']);
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");
    }

    public function testGenerateTableRowsWithCountUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows("t_source_count");

        $t1 = $this->tripodTables->getTableRows("t_source_count");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['source_count']),"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject,'http://purl.org/dc/terms/isVersionOf','http://example.com');

        $this->tripod->saveChanges($subjectGraph,$newGraph);

        $t2 = $this->tripodTables->getTableRows("t_source_count");

        $result = null;
        $this->assertEquals(count($t2['results']),2);
        foreach ($t2['results'] as $r)
        {
            if ($r['_id']['r'] = $subject) {
                $result = $r;
            }
        }

        $this->assertTrue(isset($result),"Cound not find table row for $subject");
        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['source_count']),"Result does not contain source_count");
        $this->assertEquals(2,$result['source_count']);
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");
    }

    public function testGenerateTableRowsWithCountAndRegexUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows("t_source_count_regex");

        $t1 = $this->tripodTables->getTableRows("t_source_count_regex");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['source_count']),"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertEquals(0,$result['regex_source_count']);
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject,'http://purl.org/dc/terms/isVersionOf','http://foobarbaz.com');
        $newGraph->add_resource_triple($subject,'http://purl.org/dc/terms/isVersionOf','http://example.com/foobarbaz');

        $this->tripod->saveChanges($subjectGraph,$newGraph);

        $t2 = $this->tripodTables->getTableRows("t_source_count_regex");

        $result = null;
        $this->assertEquals(count($t2['results']),2);
        foreach ($t2['results'] as $r)
        {
            if ($r['_id']['r'] = $subject) {
                $result = $r;
            }
        }

        $this->assertTrue(isset($result),"Cound not find table row for $subject");
        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['source_count']),"Result does not contain source_count");
        $this->assertEquals(3,$result['source_count']);
        $this->assertEquals(2,$result['regex_source_count']);
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");
    }

    public function testUpdateWillDeleteItem()
    {
        $mockTables = $this->getMock('MongoTripodTables', array('deleteTableRowsForResource','generateTableRowsForResource'), $this->tablesConstParams);
        $mockTables->expects($this->once())->method('deleteTableRowsForResource')->with("http://foo","context");
        $mockTables->expects($this->never())->method('generateTableRowsForResource');

        $data = array();
        $data["r"] = "http://foo";
        $data["c"] = "context";
        $data["delete"] = true;
        $mockTables->update(new ModifiedSubject($data));
    }

    public function testUpdateWillGenerateRows()
    {
        $mockTables = $this->getMock('MongoTripodTables', array('deleteRowsForResource','generateTableRowsForResource'), $this->tablesConstParams);
        $mockTables->expects($this->once())->method('generateTableRowsForResource')->with("http://foo","context");
        $mockTables->expects($this->never())->method('deleteTableRowsForResource');

        $data = array();
        $data["r"] = "http://foo";
        $data["c"] = "context";
        $mockTables->update(new ModifiedSubject($data));
    }

    public function testGenerateTableRows()
    {
        $this->tripodTables->generateTableRows("t_resource");

        $t1 = $this->tripodTables->getTableRows("t_resource");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertTrue(isset($result['type']),"Result does not contain type");
        $this->assertTrue(isset($result['isbn']),"Result does not contain isbn");
        $this->assertTrue(isset($result['isbn13']),"Result does not contain isbn13");
    }

    public function testGetTableRowsSort()
    {
        $this->tripodTables->generateTableRows("t_resource");

        $t1 = $this->tripodTables->getTableRows("t_resource",array(),array("value.isbn"=>-1));
        // expecting two rows, first row should be one with highest numberic value of ISBN, due to sort DESC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',$t1['results'][0]['_id']['r']);

        $t1 = $this->tripodTables->getTableRows("t_resource",array(),array("value.isbn"=>1));

        // expecting two rows, first row should be one with lowest numberic value of ISBN, due to sort ASC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',$t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsFilter()
    {
        $this->tripodTables->generateTableRows("t_resource");

        $t1 = $this->tripodTables->getTableRows("t_resource",array("value.isbn"=>'9780393929690')); // only bring back rows with isbn = 9780393929690

        // expecting one row
        $this->assertTrue(count($t1['results'])==1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',$t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsLimitOffset()
    {
        $this->tripodTables->generateTableRows("t_resource");

        $t1 = $this->tripodTables->getTableRows("t_resource",array(),array("value.isbn"=>1),0,1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA
        $this->assertTrue(count($t1['results'])==1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',$t1['results'][0]['_id']['r']);

        $t2 = $this->tripodTables->getTableRows("t_resource",array(),array("value.isbn"=>1),1,1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2
        $this->assertTrue(count($t2['results'])==1);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',$t2['results'][0]['_id']['r']);
    }

    public function testGenerateTableRowsForResourceUnnamespaced()
    {
        $data = array();
        $data["r"] = "http://basedata.com/b/2";
        $data["c"] = "http://basedata.com/b/DefaultGraph";
        $this->tripodTables->update(new ModifiedSubject($data));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceNamespaced()
    {
        $data = array();
        $data["r"] = "baseData:2";
        $data["c"] = "baseData:DefaultGraph";
        $this->tripodTables->update(new ModifiedSubject($data));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceContextNamespaced()
    {
        $data = array();
        $data["r"] = "http://basedata.com/b/2";
        $data["c"] = "baseData:DefaultGraph";
        $this->tripodTables->update(new ModifiedSubject($data));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceResourceNamespaced()
    {
        $data = array();
        $data["r"] = "baseData:2";
        $data["c"] = "http://basedata.com/b/DefaultGraph";
        $this->tripodTables->update(new ModifiedSubject($data));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }

    public function testGenerateTableRowsForResourcesOfTypeWithNamespace()
    {
        /* @var $mockTripodTables MongoTripodTables */
        $mockTripodTables = $this->getMock('MongoTripodTables', array('generateTableRows'), array($this->tripod->db,$this->tripod->collection,'http://talisaspire.com/'));
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->will($this->returnValue(array("ok"=>true)));

        // check where referred to as acorn:Work2 in spec...
        $mockTripodTables->generateTableRowsForType("http://talisaspire.com/schema#Work2");

        /* @var $mockTripodTables MongoTripodTables */
        $mockTripodTables = $this->getMock('MongoTripodTables', array('generateTableRows'), array($this->tripod->db,$this->tripod->collection,'http://talisaspire.com/'));
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->will($this->returnValue(array("ok"=>true)));

        // check where referred to as http://talisaspire.com/schema#Resource in spec...
        $mockTripodTables->generateTableRowsForType("acorn:Resource");
    }

    /**
     * Test table specification predicate modifier config
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersValidConfig()
    {
        // All config defined here should be valid
        $tableSpecifications = array(
            'fields' => array(
                array(
                    'fieldName' => 'test1',
                    'predicates' => array(
                        'join' => array(
                            'glue' => ';',
                            'predicates' => array('foaf:name')
                        )
                    )
                ),
                array(
                    'fieldName' => 'test2',
                    'predicates' => array(
                        'lowercase' => array(
                            'predicates' => array('foaf:name')
                        )
                    )
                ),
                array(
                    'fieldName' => 'test3',
                    'predicates' => array(
                        'lowercase' => array(
                            'join' => array(
                                'glue' => ';',
                                'predicates' => array('foaf:name')
                            )
                        )
                    )
                ),
                array(
                    'fieldName' => 'test4',
                    'predicates' => array(
                        'date' => array(
                            'predicates' => array('temp:last_login')
                        )
                    )
                )
            )
        );

        // Note that you need some config in order to create the MongoTripodConfig object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        $tripodConfig = new MongoTripodConfig($this->generateMongoTripodTestConfig());

        foreach($tableSpecifications['fields'] as $field)
        {
            // If there is invalid config, an exception will be thrown
            $this->assertNull($tripodConfig->checkModifierFunctions($field['predicates'], MongoTripodTables::$predicateModifiers), 'Invalid tablespec config');
        }

    }

    /**
     * Test invalid table specification predicate modifier config - use a bad attribute
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersInvalidConfigBadGlue()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            "Invalid modifier: 'glue2' in key 'join'"
        );

        // Create some dodgy config ("glue2") and see if an exception is thrown
        $tableSpecifications = array(
            'fieldName' => 'test1',
            'predicates' => array(
                'join' => array(
                    'glue2' => ';',
                    'predicates' => array('foaf:name')
                )
            )
        );

        // Note that you need some config in order to create the MongoTripodConfig object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        $tripodConfig = new MongoTripodConfig($this->generateMongoTripodTestConfig());
        $tripodConfig->checkModifierFunctions($tableSpecifications['predicates'], MongoTripodTables::$predicateModifiers);
    }

    /**
     * Test table rows have been generated successfully for a "join" modifier
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoin()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        $this->assertEquals('Harry Potter',$rows['results'][0]['join']);
    }

    /**
     * Test table rows have been generated for a "join" modifier but with a single value rather than an array
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinSingle()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        $this->assertEquals('Harry', $rows['results'][0]['joinSingle']);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier with a "join" inside it
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCase()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        $this->assertEquals('harry potter',$rows['results'][0]['joinLowerCase']);
    }

    /**
     * Test table rows have been generated for a "date" modifier
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDate()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        $this->assertInstanceOf('MongoDate', $rows['results'][0]['mongoDate']);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with a value that does not exist
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDateDoesNotExist()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        // Test for data that doesn't exist
        $this->assertArrayNotHasKey('mongoDateDoesNotExist', $rows['results'][0]);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier wtih a "join" modifier inside. It also has an
     * extra field attached to the row as well
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCaseAndExtraField()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        $this->assertArrayHasKey('joinLowerCaseANDExtraField', $rows['results'][0]);
        $this->assertInternalType('array', $rows['results'][0]['joinLowerCaseANDExtraField']);
        $this->assertEquals('harry potter', $rows['results'][0]['joinLowerCaseANDExtraField'][0]);
        $this->assertEquals('Harry', $rows['results'][0]['joinLowerCaseANDExtraField'][1]);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with an invalid date string
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersDateInvalid()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        // Check borked data
        // Trying to use date but passed in a string - should default to 0 for sec and usec
        $this->assertInstanceOf('MongoDate', $rows['results'][0]['mongoDateInvalid']);
        $this->assertEquals(0, $rows['results'][0]['mongoDateInvalid']->sec);
        $this->assertEquals(0, $rows['results'][0]['mongoDateInvalid']->usec);

    }

    /**
     * Test table rows have been generated for a "lowercase" modifier around a "date" modifier
     * @access public
     * @return void
     */
    public function testGenerateTableRowsForUsersWithModifiersLowercaseDate()
    {
        // Get table rows
        $rows = $this->generateTableRows("t_users");

        // We should have 1 result and it should have modified fields
        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");

        // Lowercasing a mongodate object should be the same as running a __toString() on the date object
        $this->assertEquals($rows['results'][0]['mongoDate']->__toString(), $rows['results'][0]['lowercaseDate']);
    }
}