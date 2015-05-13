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

        $this->tripod = new MongoTripod("CBD_testing", "tripod_php_testing", array("async"=>array(OP_VIEWS=>false, OP_TABLES=>false, OP_SEARCH=>false)));

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);

        $this->loadBaseDataViaTripod();

        $this->tablesConstParams = array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),'http://talisaspire.com/');

        $this->tripodTables = new MongoTripodTables($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),null); // pass null context, should default to http://talisaspire.com

        // purge tables
        foreach(MongoTripodConfig::getInstance()->getCollectionsForTables($this->tripod->getStoreName()) as $collection)
        {
            $collection->drop();
        }
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
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018"
            )
        );
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"db",
                "pods" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","data_source"=>"db");
        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"db"
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
        $this->assertArrayHasKey('type',$result,"Result does not contain type");
        $this->assertArrayHasKey('source_count',$result,"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertEquals(0,$result['random_predicate_count']);
        $this->assertArrayHasKey('isbn13',$result,"Result does not contain isbn13");
    }

    public function testGenerateTableRowsWithCountUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows("t_source_count");

        $t1 = $this->tripodTables->getTableRows("t_source_count");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type',$result,"Result does not contain type");
        $this->assertArrayHasKey('source_count',$result,"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertArrayHasKey('isbn13',$result,"Result does not contain isbn13");

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

        $this->assertNotNull($result,"Cound not find table row for $subject");
        // check out the columns
        $this->assertArrayHasKey('type',$result,"Result does not contain type");
        $this->assertArrayHasKey('source_count',$result,"Result does not contain source_count");
        $this->assertEquals(2,$result['source_count']);
        $this->assertArrayHasKey('isbn13',$result,"Result does not contain isbn13");
    }

    public function testGenerateTableRowsWithCountAndRegexUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows("t_source_count_regex");

        $t1 = $this->tripodTables->getTableRows("t_source_count_regex");

        // expecting two rows
        $this->assertEquals(count($t1['results']),2);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type',$result,"Result does not contain type");
        $this->assertArrayHasKey('source_count',$result,"Result does not contain source_count");
        $this->assertEquals(1,$result['source_count']);
        $this->assertEquals(0,$result['regex_source_count']);
        $this->assertArrayHasKey('isbn13',$result,"Result does not contain isbn13");

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

        $this->assertNotNull($result,"Cound not find table row for $subject");
        // check out the columns
        $this->assertArrayHasKey('type',$result,"Result does not contain type");
        $this->assertArrayHasKey('source_count',$result,"Result does not contain source_count");
        $this->assertEquals(3,$result['source_count']);
        $this->assertEquals(2,$result['regex_source_count']);
        $this->assertArrayHasKey('isbn13',$result,"Result does not contain isbn13");
    }

    public function testGenerateTableRowsWithCountOnJoinAndRegexUpdateAndRequery()
    {
        $this->tripodTables->generateTableRows("t_join_source_count_regex");

        $t1 = $this->tripodTables->getTableRows("t_join_source_count_regex",array("_id.r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2"));

        // expecting two rows
        $this->assertEquals(count($t1['results']),1);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count',$result,"Result does not contain titles_count");
        $this->assertEquals(3,$result['titles_count']);

        // add a title to f340...
        $subjectGraph = $this->tripod->describeResource("http://jacs3.dataincubator.org/f340");
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple("http://jacs3.dataincubator.org/f340",'http://purl.org/dc/terms/title','Another title');

        $this->tripod->saveChanges($subjectGraph,$newGraph);

        $t2 = $this->tripodTables->getTableRows("t_join_source_count_regex",array("_id.r"=>"http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2"));

        $this->assertEquals(count($t2['results']),1);
        $result = $t2['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count',$result,"Result does not contain titles_count");
        $this->assertEquals(4,$result['titles_count']);
    }

    public function testUpdateWillDeleteItem()
    {
        $mockTables = $this->getMock('MongoTripodTables', array('deleteTableRowsForResource','generateTableRowsForType'), $this->tablesConstParams);
        $mockTables->expects($this->once())->method('deleteTableRowsForResource')->with("http://foo","context");
        $mockTables->expects($this->never())->method('generateTableRowsForType');

        $mockTables->update(new ImpactedSubject(array("r"=>"http://foo","c"=>"context"),OP_TABLES,"foo","bar",array("t_table"),true));
    }

    public function testUpdateWillGenerateRows()
    {
        $mockTables = $this->getMock('MongoTripodTables', array('deleteRowsForResource','generateTableRowsForResource'), $this->tablesConstParams);
        $mockTables->expects($this->once())->method('generateTableRowsForResource')->with("http://foo","context");
        $mockTables->expects($this->never())->method('deleteTableRowsForResource');

        $mockTables->update(new ImpactedSubject(array("r"=>"http://foo","c"=>"context"),OP_TABLES,"foo","bar",array("t_table"),false));
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
        $this->tripodTables->update(new ImpactedSubject(array("r"=>"http://basedata.com/b/2","c"=>"http://basedata.com/b/DefaultGraph"),OP_TABLES,$this->tripodTables->getStoreName(),$this->tripodTables->getPodName(),array("t_work2"),false));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceNamespaced()
    {
        $this->tripodTables->update(new ImpactedSubject(array("r"=>"baseData:2","c"=>"baseData:DefaultGraph"),OP_TABLES,$this->tripodTables->getStoreName(),$this->tripodTables->getPodName(),array("t_work2"),false));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceContextNamespaced()
    {
        $this->tripodTables->update(new ImpactedSubject(array("r"=>"http://basedata.com/b/2","c"=>"baseData:DefaultGraph"),OP_TABLES,$this->tripodTables->getStoreName(),$this->tripodTables->getPodName(),array("t_work2"),false));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }
    public function testGenerateTableRowsForResourceResourceNamespaced()
    {
        $this->tripodTables->update(new ImpactedSubject(array("r"=>"baseData:2","c"=>"http://basedata.com/b/DefaultGraph"),OP_TABLES,$this->tripodTables->getStoreName(),$this->tripodTables->getPodName(),array("t_work2"),false));

        $rows = $this->tripodTables->getTableRows("t_work2");

        $this->assertTrue($rows["head"]["count"]==1,"Expected one row");
    }

    public function testGenerateTableRowsForResourcesOfTypeWithNamespace()
    {
        /* @var MongoTripodTables|PHPUnit_Framework_MockObject_MockObject $mockTripodTables  */
        $mockTripodTables = $this->getMock(
            'MongoTripodTables',
            array('generateTableRows'),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),'http://talisaspire.com/')
        );
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->will($this->returnValue(array("ok"=>true)));

        // check where referred to as acorn:Work2 in spec...
        $mockTripodTables->generateTableRowsForType("http://talisaspire.com/schema#Work2");

        /* @var MongoTripodTables|PHPUnit_Framework_MockObject_MockObject $mockTripodTables */
        $mockTripodTables = $this->getMock(
            'MongoTripodTables',
            array('generateTableRows'),
            array($this->tripod->getStoreName(),$this->getTripodCollection($this->tripod),'http://talisaspire.com/')
        );
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
            _ID_KEY => 't_testGenerateTableRowsForUsersWithModifiersValidConfig',
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
        MongoTripodConfig::setConfig($this->generateMongoTripodTestConfig());
        $tripodConfig = MongoTripodConfig::getInstance();

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
            _ID_KEY => 't_foo',
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
        MongoTripodConfig::setConfig($this->generateMongoTripodTestConfig());
        $tripodConfig = MongoTripodConfig::getInstance();

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

    /**
     * Test that link modifier is derived from the joined resource id, rather than base
     * @access public
     * @return void
     */
    public function testJoinLinkValueIsForJoinedResource()
    {
        $this->tripodTables->generateTableRows("t_join_link");
        $rows = $this->tripodTables->getTableRows("t_join_link",array("_id.r"=>"baseData:foo1234"));
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertArrayHasKey('knowsLink', $rows['results'][0]);
        $this->assertArrayHasKey('workLink', $rows['results'][0]);
        // Check bookLink values
        $this->assertEquals("baseData:foo1234", $rows['results'][0]['_id']['r']);
        $this->assertEquals("http://basedata.com/b/foo1234", $rows['results'][0]['bookLink']);

        // Check authorLink values
        $this->assertEquals("user:10101", $rows['results'][0]['authorUri']);
        $this->assertEquals("http://schemas.talis.com/2005/user/schema#10101", $rows['results'][0]['authorLink']);

        // Check knowsLink values
        $this->assertEquals("user:10102", $rows['results'][0]['knowsUri']);
        $this->assertEquals("http://schemas.talis.com/2005/user/schema#10102", $rows['results'][0]['knowsLink']);

        // Check workLink values
        $this->assertEquals("http://talisaspire.com/works/4d101f63c10a6", $rows['results'][0]['workUri']); // Already a fq URI
        $this->assertEquals("http://talisaspire.com/works/4d101f63c10a6", $rows['results'][0]['workLink']);
    }

    /**
     * Test to ensure that impact index contains joined ids for resources that do not yet exist in the database (i.e.
     * allow open world model)
     * @access public
     * @return void
     */
    public function testPreviouslyUnavailableDataBecomesPresentAndTriggersTableRegen()
    {
        $this->tripodTables->generateTableRows("t_join_link");
        $rows = $this->tripodTables->getTableRows("t_join_link",array("_id.r"=>"baseData:bar1234"));
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertEquals("user:10103", $rows['results'][0]['authorUri']);
        // Author link should not appear because resource has not yet been created
        $this->assertArrayNotHasKey('authorLink', $rows['results'][0]);

        $uri = 'http://schemas.talis.com/2005/user/schema#10103';
        // Confirm this user does not exist
        $this->assertFalse($this->tripod->describeResource($uri)->has_triples_about($uri));

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri("rdf:type"), $g->qname_to_uri("foaf:Person"));
        $g->add_literal_triple($uri, $g->qname_to_uri("foaf:name"), "A. Nonymous");
        $this->tripod->saveChanges(new MongoGraph(), $g,"http://talisaspire.com/", "This resource didn't exist at join time");

        $userGraph = $this->tripod->describeResource($uri);

        $this->assertTrue($userGraph->has_triples_about($uri), "new entity we created was not saved");

        // Get our table rows again
        $rows = $this->tripodTables->getTableRows("t_join_link",array("_id.r"=>"baseData:bar1234"));
        // authorLink should now be populated
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertEquals($uri, $rows['results'][0]['authorLink']);
    }

    /**
     * Ensure that an array of links is returned if there are multiple resources matched by the join
     * @access public
     * @return void
     */
    public function testLinkWorksOnRepeatingPredicatesForResource()
    {
        $this->tripodTables->generateTableRows("t_link_multiple");
        $rows = $this->tripodTables->getTableRows("t_link_multiple",array("_id.r"=>"baseData:bar1234"));
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('contributorLink', $rows['results'][0]);
        $this->assertTrue(is_array($rows['results'][0]['contributorLink']));
        $this->assertEquals(2, count($rows['results'][0]['contributorLink']));
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10101', $rows['results'][0]['contributorLink'][0]);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10102', $rows['results'][0]['contributorLink'][1]);
    }

    /**
     * Return the distinct values of a table column
     * @access public
     * @return void
     */
    public function testDistinct()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $rows = $this->tripodTables->getTableRows($table, array(), array(), 0, 0);
        $this->assertEquals(7, $rows['head']['count']);
        $results = $this->tripodTables->distinct($table, "value.title");

        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(3, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(3, count($results['results']));
        $this->assertContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        // Supply a filter
        $results = $this->tripodTables->distinct($table, "value.title", array('value.type'=>"bibo:Document"));
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(2, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(2, count($results['results']));
        $this->assertNotContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        $results = $this->tripodTables->distinct($table, "value.type");
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(4, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEquals(4, count($results['results']));
        $this->assertContains('acorn:Resource', $results['results']);
        $this->assertContains('acorn:Work', $results['results']);
        $this->assertContains('bibo:Book', $results['results']);
        $this->assertContains('bibo:Document', $results['results']);
    }

    /**
     * Return no results for tablespec that doesn't exist
     * @access public
     * @return void
     */
    public function testDistinctOnTableSpecThatDoesNotExist()
    {
        $table = "t_nothing_to_see_here";
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Table id \'t_nothing_to_see_here\' not in configuration'
        );
        $results = $this->tripodTables->distinct($table, "value.foo");
    }

    /**
     * Return no results for distinct on a fieldname that is not defined in tableSpec
     * @access public
     * @return void
     */
    public function testDistinctOnFieldNameThatIsNotInTableSpec()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, "value.foo");
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**
     * Return no results for filters that match no table rows
     * @access public
     * @return void
     */
    public function testDistinctForFilterWithNoMatches()
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, "value.title", array('value.foo'=>"wibble"));
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    public function testTableRowsGenerateWhenDefinedPredicateChanges()
    {
        foreach(MongoTripodConfig::getInstance()->getTableSpecifications($this->tripod->getStoreName()) as $specId=>$spec)
        {
            $this->generateTableRows($specId);
        }

        $context = 'http://talisaspire.com/';
        $uri = "http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2";

        /** @var PHPUnit_Framework_MockObject_MockObject|MongoTripod $tripod */
        $tripod = $this->getMock(
            'MongoTripod',
            array('getComposite'),
            array(
                'CBD_testing',
                'tripod_php_testing',
                array(
                    'defaultContext'=>$context,
                    'async'=>array(
                        OP_VIEWS=>true,
                        OP_TABLES=>false,
                        OP_SEARCH=>true
                    )
                )
            )
        );

        $labeller = new MongoTripodLabeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:title")
        );

        $tables = $this->getMock('MongoTripodTables',
            array('generateTableRows'),
            array(
                $tripod->getStoreName(),
                $this->getTripodCollection($tripod),
                "http://talisaspire.com/"
            )
        );

        $tables->expects($this->exactly(2))
            ->method('generateTableRows')
            ->withConsecutive(
                array(
                    $this->equalTo('t_distinct'),
                    $this->equalTo($uri),
                    $this->equalTo($context)
                ),
                array(
                    $this->equalTo('t_join_source_count_regex'),
                    $this->equalTo($uri),
                    $this->equalTo($context)
                )
            );


        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->will($this->returnValue($tables));

        // Walk through the processSyncOperations process manually for tables

        /** @var MongoTripodTables $table */
        $table = $tripod->getComposite(OP_TABLES);
        $this->assertInstanceOf('MongoTripodTables', $table);

        $expectedImpactedSubject = new ImpactedSubject(
            array(
                _ID_RESOURCE=>$labeller->uri_to_alias($uri),
                _ID_CONTEXT=>$context
            ),
            OP_TABLES,
            'tripod_php_testing',
            'CBD_testing',
            array("t_join_source_count_regex", "t_distinct")
        );

        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEquals($expectedImpactedSubject, $impactedSubjects[0]);

        foreach($impactedSubjects as $subject)
        {
            $table->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated table.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        $collections = MongoTripodConfig::getInstance()->getCollectionsForTables('tripod_php_testing');
        foreach($collections as $collection)
        {
            $query = array(
                'value._impactIndex'=>array('r'=>$labeller->uri_to_alias($uri), 'c'=>$context),
                '_id.type'=>array('$in'=>array("t_distinct", "t_join_source_count_regex", ))
            );
            $this->assertEquals(0, $collection->count($query));
        }
    }

    public function testTableRowsNotGeneratedWhenUndefinedPredicateChanges()
    {
        foreach(MongoTripodConfig::getInstance()->getTableSpecifications($this->tripod->getStoreName()) as $specId=>$spec)
        {
            $this->generateTableRows($specId);
        }

        $context = 'http://talisaspire.com/';
        $uri = "http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2";

        $labeller = new MongoTripodLabeller();
        $subjectsAndPredicatesOfChange = array(
            $labeller->uri_to_alias($uri)=>array("dct:description")
        );

        // Walk through the processSyncOperations process manually for tables

        $table = new MongoTripodTables(
            'tripod_php_testing',
            MongoTripodConfig::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
            $context
        );


        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $context);

        $this->assertEmpty($impactedSubjects);
    }


}