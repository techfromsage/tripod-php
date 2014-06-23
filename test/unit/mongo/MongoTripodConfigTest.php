<?php
require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/MongoTripodConfig.class.php';

class MongoTripodConfigTest extends MongoTripodTestBase
{
    /**
     * @var MongoTripodConfig
     */
    private $tripodConfig = null;

    protected function setUp()
    {
        parent::setup();
        $this->tripodConfig = MongoTripodConfig::getInstance();
    }

    public function testGetInstanceThrowsExceptionIfSetInstanceNotCalledFirst()
    {
        // to test that the instance throws an exception if it is called before calling setConfig
        // i first have to destroy the instance that is created in the setUp() method of our test suite.

        $this->setExpectedException('MongoTripodConfigException','Call MongoTripodConfig::setConfig() first');
        unset($this->tripodConfig);

        MongoTripodConfig::getInstance()->destroy();
        MongoTripodConfig::getInstance();
    }

    public function testNamespaces()
    {
        $ns = $this->tripodConfig->ns;
        $this->assertEquals(16,count($ns),"Incorrect number of namespaces");

        $expectedNs = array();

        $expectedNs['rdf'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
        $expectedNs['dct'] = 'http://purl.org/dc/terms/';
        $expectedNs['resourcelist'] = 'http://purl.org/vocab/resourcelist/schema#';
        $expectedNs['temp'] = 'http://lists.talis.com/schema/temp#';
        $expectedNs['spec'] = 'http://rdfs.org/sioc/spec/';
        $expectedNs['events'] = 'http://schemas.talis.com/2009/events/';
        $expectedNs['acorn'] = 'http://talisaspire.com/schema#';
        $expectedNs['searchterms'] = 'http://talisaspire.com/searchTerms/schema#';
        $expectedNs['opensearch'] = 'http://a9.com/-/opensearch/extensions/relevance/1.0/';
        $expectedNs['sioc'] = 'http://rdfs.org/sioc/ns#';
        $expectedNs['aiiso'] = 'http://purl.org/vocab/aiiso/schema#';
        $expectedNs['user'] = 'http://schemas.talis.com/2005/user/schema#';
        $expectedNs['changeset'] = 'http://purl.org/vocab/changeset/schema#';
        $expectedNs['bibo'] = 'http://purl.org/ontology/bibo/';
        $expectedNs['foaf'] = 'http://xmlns.com/foaf/0.1/';
        $expectedNs['baseData'] = 'http://basedata.com/b/';
        $this->assertEquals($expectedNs,$ns,"Incorrect namespaces");
    }

    public function testTConfig()
    {
        $config = MongoTripodConfig::getInstance();
        $this->assertEquals('testing',$config->tConfig['database']);
        $this->assertEquals('transaction_log',$config->tConfig['collection']);
        $this->assertEquals('mongodb://localhost',$config->getTransactionLogConnStr());
    }

    public function testTConfigRepSetConnStr()
    {
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost",
                "collections" => array(
                    "CBD_testing" => array()
                ),
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","connStr"=>"mongodb://localhost");
        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "connStr"=>"mongodb://tloghost:27017,tloghost:27018/admin",
            "replicaSet" => "tlogrepset"
        );

        $mtc = new MongoTripodConfig($config);
        $this->assertEquals("mongodb://tloghost:27017,tloghost:27018/admin",$mtc->getTransactionLogConnStr());
    }

    public function testTConfigRepSetConnStrThrowsException()
    {
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for Transaction Log must include /admin database when connecting to Replica Set');

        $config=array();
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
            "connStr"=>"mongodb://tloghost:27017,tloghost:27018",
            "replicaSet" => "tlogrepset"
        );

        $mtc = new MongoTripodConfig($config);
        $connStr = $mtc->getTransactionLogConnStr();
    }

    public function testCardinality()
    {
        $cardinality = $this->tripodConfig->getCardinality("testing","CBD_testing","dct:created");
        $this->assertEquals(1,$cardinality,"Expected cardinality of 1 for dct:created");

        $cardinality = $this->tripodConfig->getCardinality("testing","CBD_testing","random:property");
        $this->assertEquals(-1,$cardinality,"Expected cardinality of 1 for random:property");
    }

    public function testGetConnectionString()
    {
        $this->assertEquals("mongodb://localhost",MongoTripodConfig::getInstance()->getConnStr("testing"));
    }

    public function testGetConnectionStringThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Database notexists does not exist in configuration');
        $this->assertEquals("mongodb://localhost",MongoTripodConfig::getInstance()->getConnStr("notexists"));
    }

    public function testGetConnectionStringForReplicaSet(){
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost:27017,localhost:27018/admin",
                "collections" => array(
                    "CBD_testing" => array()
                ),
                "replicaSet" => "myrepset"
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");

        $mtc = new MongoTripodConfig($config);
        $this->assertEquals("mongodb://localhost:27017,localhost:27018/admin",$mtc->getConnStr("testing"));
    }

    public function testGetConnectionStringThrowsExceptionForReplicaSet(){
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for testing must include /admin database when connecting to Replica Set');
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost:27017,localhost:27018",
                "collections" => array(
                    "CBD_testing" => array()
                ),
                "replicaSet" => "myrepset"
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");

        $mtc = new MongoTripodConfig($config);
        $mtc->getConnStr("testing");
    }

    public function testCompoundIndexAllArraysThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Compound index IllegalCompoundIndex has more than one field with cardinality > 1 - mongo will not be able to build this index');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                        "indexes"=>array(
                            "IllegalCompoundIndex"=>array(
                                "rdf:type.value"=>1,
                                "dct:subject.value"=>1)
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");

        $mtc = new MongoTripodConfig($config);
    }

    public function testSearchConfig()
    {
        $config = MongoTripodConfig::getInstance();
        $this->assertEquals('MongoSearchProvider', $config->searchProvider);
        $this->assertEquals(2, count($config->searchDocSpecs));
    }

    public function testQueueConfig()
    {
        $config = MongoTripodConfig::getInstance();
        $this->assertEquals('testing',$config->queue['database']);
        $this->assertEquals('q_queue',$config->queue['collection']);
        $this->assertEquals('mongodb://localhost',$config->getQueueConnStr());
    }

    public function testQueueRepSetConnStr()
    {
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost",
                "collections" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config['queue'] = array(
            "database"=>"queue",
            "collection"=>"q_queue",
            "connStr"=>"mongodb://qhost:27017,qhost:27018/admin",
            "replicaSet" => "myrepset"
        );

        $mtc = new MongoTripodConfig($config);
        $this->assertEquals("mongodb://qhost:27017,qhost:27018/admin",$mtc->getQueueConnStr());
    }

    public function testQueueRepSetConnStrThrowsException()
    {
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for Queue must include /admin database when connecting to Replica Set');

        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost",
                "collections" => array(
                    "CBD_testing" => array()
                ),
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config['queue'] = array(
            "database"=>"queue",
            "collection"=>"q_queue",
            "connStr"=>"mongodb://qhost:27017,qhost:27018",
            "replicaSet" => "myrepset"
        );

        $mtc = new MongoTripodConfig($config);
        $connStr = $mtc->getQueueConnStr();
    }

    public function testCardinalityRuleWithNoNamespace()
    {
        $this->setExpectedException('MongoTripodConfigException', "Cardinality 'foo:bar' does not have the namespace defined");

        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://talisaspire:acorn123@46.137.106.66:27018");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                        "cardinality"=>array(
                            "foo:bar"=>1
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testGetSearchDocumentSpecificationsByType()
    {
        $expectedSpec = array(
            array(
                "_id"=>"i_search_list",
                "type"=>array("resourcelist:List"),
                "from"=>"CBD_testing",
                "filter"=>array(
                    array("condition"=>array(
                        "spec:name.l"=>array('$exists'=>true)
                    ))
                ),
                "indices"=>array(
                    array(
                        "fieldName"=>"search_terms",
                        "predicates"=>array("spec:name","resourcelist:description")
                    )
                ),
                "fields"=>array(
                    array(
                        "fieldName"=>"result.title",
                        "predicates"=>array("spec:name"),
                        "limit"=>1
                    ),
                    array(
                        "fieldName"=>"result.link",
                        "value"=>"_link_",
                    )
                ),
                "joins"=>array(
                    "resourcelist:usedBy"=>array(
                        "indices"=>array(
                            array(
                                "fieldName"=>"search_terms",
                                "predicates"=>array("aiiso:name","aiiso:code")
                            )
                        )
                    )
                )
            )
        );
        $actualSpec = MongoTripodConfig::getInstance()->getSearchDocumentSpecifications("resourcelist:List");
        $this->assertEquals($expectedSpec,$actualSpec);
    }

    public function testGetSearchDocumentSpecificationsById()
    {
        $expectedSpec =
            array(
                "_id"=>"i_search_list",
                "type"=>array("resourcelist:List"),
                "from"=>"CBD_testing",
                "filter"=>array(
                    array("condition"=>array(
                        "spec:name.l"=>array('$exists'=>true)
                    ))
                ),
                "indices"=>array(
                    array(
                        "fieldName"=>"search_terms",
                        "predicates"=>array("spec:name","resourcelist:description")
                    )
                ),
                "fields"=>array(
                    array(
                        "fieldName"=>"result.title",
                        "predicates"=>array("spec:name"),
                        "limit"=>1
                    ),
                    array(
                        "fieldName"=>"result.link",
                        "value"=>"_link_",
                    )
                ),
                "joins"=>array(
                    "resourcelist:usedBy"=>array(
                        "indices"=>array(
                            array(
                                "fieldName"=>"search_terms",
                                "predicates"=>array("aiiso:name","aiiso:code")
                            )
                        )
                    )
                )
            );
        $actualSpec = MongoTripodConfig::getInstance()->getSearchDocumentSpecification("i_search_list");
        $this->assertEquals($expectedSpec,$actualSpec);
    }


    public function testGetSearchDocumentSpecificationsWhereNoneExists()
    {
        $expectedSpec = array();
        $actualSpec = MongoTripodConfig::getInstance()->getSearchDocumentSpecifications("something:doesntexist");
        $this->assertEquals($expectedSpec,$actualSpec);
    }

    public function testViewSpecCountWithoutTTLThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Aggregate function counts exists in spec, but no TTL defined');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["view_specifications"] = array(
            array(
                "_id"=>"v_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "counts"=>array(
                    "acorn:resourceCount"=>array(
                        "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                        "property"=>"dct:isVersionOf"
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testViewSpecCountNestedInJoinWithoutTTLThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Aggregate function counts exists in spec, but no TTL defined');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["view_specifications"] = array(
            array(
                "_id"=>"v_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "joins"=>array(
                    "acorn:seeAlso"=>array(
                        "counts"=>array(
                            "acorn:resourceCount"=>array(
                                "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                                "property"=>"dct:isVersionOf"
                            )
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecNestedCountThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Aggregate function counts exists within join in table spec, this is not allowed');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "joins"=>array(
                    "acorn:resourceCount"=>array(
                        "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                        "property"=>"dct:isVersionOf",
                        "counts"=>array(array("fieldName"=>"some","property"=>"value"))
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecNested2ndLevelCountThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Aggregate function counts exists within join in table spec, this is not allowed');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "joins"=>array(
                     "acorn:resourceCount"=>array(
                         "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                         "property"=>"dct:isVersionOf",
                         "joins"=>array(
                             "another:property"=>array(
                                 "counts"=>array(array("fieldName"=>"some","property"=>"value"))
                             )
                         )
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecFieldWithoutFieldName()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Field spec does not contain fieldName');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "fields"=>array(
                     array(
                         "predicates"=>array("rdf:type"),
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecFieldWithoutPredicates()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Field spec does not contain predicates');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "fields"=>array(
                     array(
                         "fieldName"=>"some_field",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecCountWithoutProperty()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain property');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "counts"=>array(
                     array(
                         "fieldName"=>"some_field",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecCountWithoutFieldName()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain fieldName');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "counts"=>array(
                     array(
                         "property"=>"some:property",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testTableSpecCountWithoutPropertyAsAString()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec property was not a string');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
            "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "counts"=>array(
                     array(
                         "fieldName"=>"someField",
                         "property"=>array("some:property"),
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    public function testConfigWithoutDefaultNamespaceThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Mandatory config key [defaultContext] is missing from config');
        $config = array();
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["view_specifications"] = array(
            array(
                "_id"=>"v_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "joins"=>array(
                    "acorn:seeAlso"=>array(
                        "counts"=>array(
                            "acorn:resourceCount"=>array(
                                "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                                "property"=>"dct:isVersionOf"
                            )
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
    }

    /**
     * the indexesGroupedByCollection method should not only return each of the indexes that are defined explicitly in the config.json,
     * but also include indexes that are inserted by MongoTripodConfig object because they are needed by tripod
     */
    public function testGetIndexesGroupedByCollection()
    {
        $indexSpecs = MongoTripodConfig::getInstance()->getIndexesGroupedByCollection("testing");
        //print_r($indexSpecs);
        $this->assertArrayHasKey("CBD_testing", $indexSpecs);
        $this->assertArrayHasKey("index1", $indexSpecs["CBD_testing"]);
        $this->assertArrayHasKey("dct:subject.u", $indexSpecs["CBD_testing"]["index1"]);
        $this->assertArrayHasKey("index2", $indexSpecs["CBD_testing"]);
        $this->assertArrayHasKey("rdf:type.u", $indexSpecs["CBD_testing_2"]["index1"]);

        $this->assertArrayHasKey(_LOCKED_FOR_TRANS_INDEX, $indexSpecs["CBD_testing"]);
        $this->assertArrayHasKey("_id", $indexSpecs["CBD_testing"][_LOCKED_FOR_TRANS_INDEX]);
        $this->assertArrayHasKey(_LOCKED_FOR_TRANS, $indexSpecs["CBD_testing"][_LOCKED_FOR_TRANS_INDEX]);

        $this->assertArrayHasKey("CBD_testing_2", $indexSpecs);
        $this->assertArrayHasKey("index1", $indexSpecs["CBD_testing"]);
        $this->assertArrayHasKey("rdf:type.u", $indexSpecs["CBD_testing_2"]["index1"]);

        $this->assertArrayHasKey(_LOCKED_FOR_TRANS_INDEX, $indexSpecs["CBD_testing_2"]);
        $this->assertArrayHasKey("_id", $indexSpecs["CBD_testing_2"][_LOCKED_FOR_TRANS_INDEX]);
        $this->assertArrayHasKey(_LOCKED_FOR_TRANS, $indexSpecs["CBD_testing_2"][_LOCKED_FOR_TRANS_INDEX]);

        $this->assertEquals(array("value.isbn"=>1), $indexSpecs[TABLE_ROWS_COLLECTION][0]);
        $this->assertEquals(array("value._graphs.sioc:has_container.u"=>1,"value._graphs.sioc:topic.u"=>1), $indexSpecs[VIEWS_COLLECTION][0]);
    }

    public function testGetReplicaSetName()
    {
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://talisaspire:acorn123@46.137.106.66:27018");
        $config["databases"] = array(
            "testing"=>array(
                "connStr"=>"sometestval",
                "replicaSet"=>"myreplicaset",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            ),
            "testing_2"=>array(
                "connStr"=>"sometestval",
                "collections"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $mtc = new MongoTripodConfig($config);
        $this->assertEquals("myreplicaset", $mtc->getReplicaSetName("testing"));

        $this->assertNull($mtc->getReplicaSetName("testing_2"));
    }

    public function testGetViewSpecification(){
        $expectedVspec = array(
            "_id"=> "v_resource_full",
            "_version" => "0.1",
            "from"=>"CBD_testing",
            "ensureIndexes" =>array(
                array(
                    "value._graphs.sioc:has_container.u"=>1,
                    "value._graphs.sioc:topic.u"=>1
                )
            ),
            "type"=>"acorn:Resource",
            "include"=>array("rdf:type","searchterms:topic"),
            "joins"=>array(
                "dct:isVersionOf"=>array(
                    "include"=>array(
                        "dct:subject",
                        "rdf:type"
                    )
                )
            )
        );

        $vspec = MongoTripodConfig::getInstance()->getViewSpecification("v_resource_full");
        $this->assertEquals($expectedVspec, $vspec);

        $vspec = MongoTripodConfig::getInstance()->getViewSpecification("doesnt_exist");
        $this->assertNull($vspec);
    }

    public function testGetTableSpecification()
    {
        $expectedTspec = array(
            "_id"=>"t_resource",
            "type"=>"acorn:Resource",
            "from"=>"CBD_testing",
            "ensureIndexes" => array(array("value.isbn"=>1)),
            "fields"=>array(
                array(
                    "fieldName"=>"type",
                    "predicates"=>array("rdf:type")
                ),
                array(
                    "fieldName"=>"isbn",
                    "predicates"=>array("bibo:isbn13")
                ),
            ),
            "joins"=>array(
                "dct:isVersionOf"=>array(
                    "fields"=>array(
                        array(
                            "fieldName"=>"isbn13",
                            "predicates"=>array("bibo:isbn13")
                        )
                    )
                )
            )
        );

        $tspec = MongoTripodConfig::getInstance()->getTableSpecification("t_resource");
        $this->assertEquals($expectedTspec, $tspec);

        $tspec = MongoTripodConfig::getInstance()->getTableSpecification("doesnt_exist");
        $this->assertNull($tspec);
    }


    public function testSearchConfigNotPresent()
    {
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["databases"] = array(
            "testing" => array(
                "connStr" => "mongodb://localhost",
                "collections" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","connStr"=>"mongodb://localhost");
        $config['queue'] = array(
            "database"=>"testing_queue",
            "collection"=>"q_queue",
            "connStr"=>"mongodb://qhost:27017,qhost:27018/admin",
            "replicaSet" => "myrepset"
        );

        $mtc = new MongoTripodConfig($config);
        $this->assertNull($mtc->searchProvider);
        $this->assertEquals(array(), $mtc->searchDocSpecs);
    }

    public function testGetAllTypesInSpecifications()
    {
        $types = $this->tripodConfig->getAllTypesInSpecifications();
        $this->assertEquals(7, count($types), "There should be 7 types based on the configured view, table and search specifications in config.json");
        $expectedValues = array(
            "acorn:Resource",
            "acorn:Work",
            "http://talisaspire.com/schema#Work2",
            "acorn:Work2",
            "bibo:Book",
            "resourcelist:List",
            "spec:User"
        );

        foreach($expectedValues as $expected){
            $this->assertContains($expected, $types, "List of types should have contained $expected");
        }
    }


}