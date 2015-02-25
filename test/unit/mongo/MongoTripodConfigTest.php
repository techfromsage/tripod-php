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
        $ns = $this->tripodConfig->getNamespaces();
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
        $cfg = MongoTripodConfig::getConfig();
        $tConfig = $config->getTransactionLogConfig();
        $this->assertEquals('tripod_php_testing',$tConfig['database']);
        $this->assertEquals('transaction_log',$tConfig['collection']);
        $this->assertEquals($cfg['data_sources'][$cfg['transaction_log']['data_source']]['connection'],$config->getTransactionLogConnStr());
    }

    public function testTConfigRepSetConnStr()
    {
        $config=array();
        $config["data_sources"] = array(
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018/admin",
                "replicaSet" => "tlogrepset"
            ),
            "mongo"=>array("type"=>"mongo","connection" => "mongodb://localhost")
        );
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "type"=>"mongo",
                "data_source"=>"mongo",
                "pods" => array(
                    "CBD_testing" => array()
                ),
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","data_source"=>"mongo");
        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"tlog"

        );

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
        $this->assertEquals("mongodb://tloghost:27017,tloghost:27018/admin",$mtc->getTransactionLogConnStr());
    }

    public function testTConfigRepSetConnStrThrowsException()
    {
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for \'rs1\' must include /admin database when connecting to Replica Set');

        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "mongo1"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "rs1"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://tloghost:27017,tloghost:27018",
                "replicaSet" => "tlogrepset"
            )
        );
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"mongo1",
                "pods" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","data_source"=>"mongo1");
        $config["transaction_log"] = array(
            "database"=>"transactions",
            "collection"=>"transaction_log",
            "data_source"=>"rs1"
        );

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();

        $connStr = $mtc->getTransactionLogConnStr();
    }

    public function testCardinality()
    {
        $cardinality = $this->tripodConfig->getCardinality("tripod_php_testing","CBD_testing","dct:created");
        $this->assertEquals(1,$cardinality,"Expected cardinality of 1 for dct:created");

        $cardinality = $this->tripodConfig->getCardinality("tripod_php_testing","CBD_testing","random:property");
        $this->assertEquals(-1,$cardinality,"Expected cardinality of 1 for random:property");
    }

    public function testGetConnectionString()
    {
        $this->assertEquals("mongodb://localhost",MongoTripodConfig::getInstance()->getConnStr("tripod_php_testing"));
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
        $config["data_sources"] = array(
            "rs"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost:27017,localhost:27018/admin",
                "replicaSet" => "myrepset"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"rs");
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "pods" => array(
                    "CBD_testing" => array()
                ),
                "data_source"=>"rs"
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"rs");

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();

        $this->assertEquals("mongodb://localhost:27017,localhost:27018/admin",$mtc->getConnStr("tripod_php_testing"));
    }

    public function testGetConnectionStringThrowsExceptionForReplicaSet(){
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for \'rs1\' must include /admin database when connecting to Replica Set');
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "mongo1"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "rs1"=>array(
                "type"=>"mongo",
                "connection" => "mongodb://localhost:27017,localhost:27018",
                "replicaSet" => "myrepset"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"mongo1");
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"rs1",
                "pods" => array(
                    "CBD_testing" => array()
                ),
            )
        );
        $config['queue'] = array("database"=>"queue","collection"=>"q_queue","data_source"=>"mongo1");

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();

        $mtc->getConnStr("tripod_php_testing");
    }

    public function testCompoundIndexAllArraysThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Compound index IllegalCompoundIndex has more than one field with cardinality > 1 - mongo will not be able to build this index');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db1"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "db2"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db1");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db2",
                "pods"=>array(
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
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db1");

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testSearchConfig()
    {
        $config = MongoTripodConfig::getInstance();
        $this->assertEquals('MongoSearchProvider', $config->getSearchProviderClassName('tripod_php_testing'));

        $this->assertEquals(3, count($config->getSearchDocumentSpecifications('tripod_php_testing')));
    }

    public function testQueueConfig()
    {
        $config = MongoTripodConfig::getInstance();
        $queueConfig = $config->getQueueConfig();
        $this->assertEquals('tripod_php_testing',$queueConfig['database']);
        $this->assertEquals('q_queue',$queueConfig['collection']);
        $this->assertEquals('mongodb://localhost',$config->getQueueConnStr());
    }

    public function testQueueRepSetConnStr()
    {
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config['data_sources'] = array(
            'mongo'=>array(
                'type'=>'mongo',
                'connection'=>'mongodb://localhost'
            ),
            'queue'=>array(
                'type'=>'mongo',
                "connection"=>"mongodb://qhost:27017,qhost:27018/admin",
                "replicaSet" => "myrepset"
            )
        );
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"mongo",
                "pods" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"mongo");
        $config['queue'] = array(
            "database"=>"queue",
            "collection"=>"q_queue",
            "data_source"=>"queue"
        );

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
        $this->assertEquals("mongodb://qhost:27017,qhost:27018/admin",$mtc->getQueueConnStr());
    }

    public function testQueueRepSetConnStrThrowsException()
    {
        $this->setExpectedException(
                   'MongoTripodConfigException',
                   'Connection string for \'queue\' must include /admin database when connecting to Replica Set');

        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db1" => array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            ),
            "queue" => array(
                "type"=>"mongo",
                "connection"=>"mongodb://qhost:27017,qhost:27018",
                "replicaSet" => "myrepset"
            ),
        );
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"db1",
                "pods" => array(
                    "CBD_testing" => array()
                ),
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db1");
        $config['queue'] = array(
            "database"=>"queue",
            "collection"=>"q_queue",
            "data_source"=>"queue"

        );

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
        $connStr = $mtc->getQueueConnStr();
    }

    public function testCardinalityRuleWithNoNamespace()
    {
        $this->setExpectedException('MongoTripodConfigException', "Cardinality 'foo:bar' does not have the namespace defined");

        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                        "cardinality"=>array(
                            "foo:bar"=>1
                        )
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testGetSearchDocumentSpecificationsByType()
    {
        $expectedSpec = array(
            array(
                "_id"=>"i_search_list",
                "type"=>array("resourcelist:List"),
                "from"=>"CBD_testing",
                "to_data_source"=>"rs1", // This should be added automatically
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
        $actualSpec = MongoTripodConfig::getInstance()->getSearchDocumentSpecifications("tripod_php_testing", "resourcelist:List");
        $this->assertEquals($expectedSpec,$actualSpec);
    }

    public function testGetSearchDocumentSpecificationsById()
    {
        $expectedSpec =
            array(
                "_id"=>"i_search_list",
                "type"=>array("resourcelist:List"),
                "from"=>"CBD_testing",
                "to_data_source"=>"rs1", // this is added automatically
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
        $actualSpec = MongoTripodConfig::getInstance()->getSearchDocumentSpecification('tripod_php_testing', "i_search_list");
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
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["stores"]["tripod_php_testing"]["view_specifications"] = array(
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
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testViewSpecCountNestedInJoinWithoutTTLThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Aggregate function counts exists in spec, but no TTL defined');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["stores"]["tripod_php_testing"]["view_specifications"] = array(
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
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecNestedCountWithoutPropertyThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain property');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_counts",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "joins"=>array(
                    "acorn:resourceCount"=>array(
                        "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                        "property"=>"dct:isVersionOf",
                        "counts"=>array(array("fieldName"=>"someField"))
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecNested2ndLevelCountWithoutFieldNameThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain fieldName');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_counts",
                "from"=>"CBD_testing",
                "type"=>"http://talisaspire.com/schema#Work",
                "joins"=>array(
                     "acorn:resourceCount"=>array(
                         "filter"=>array("rdf:type.value"=>"http://talisaspire.com/schema#Resource"),
                         "property"=>"dct:isVersionOf",
                         "joins"=>array(
                             "another:property"=>array(
                                 "counts"=>array(array("property"=>"value"))
                             )
                         )
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecFieldWithoutFieldName()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Field spec does not contain fieldName');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "fields"=>array(
                     array(
                         "predicates"=>array("rdf:type"),
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecFieldWithoutPredicates()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Field spec does not contain predicates');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "fields"=>array(
                     array(
                         "fieldName"=>"some_field",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecCountWithoutProperty()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain property');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "counts"=>array(
                     array(
                         "fieldName"=>"some_field",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecCountWithoutFieldName()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec does not contain fieldName');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
            "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "counts"=>array(
                     array(
                         "property"=>"some:property",
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testTableSpecCountWithoutPropertyAsAString()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Count spec property was not a string');
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
            "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array()
                )
            )
        );

        $config["stores"]["tripod_php_testing"]["table_specifications"] = array(
            array(
                "_id"=>"t_illegal_spec",
                "type"=>"http://talisaspire.com/schema#Work",
                "from"=>"CBD_testing",
                "counts"=>array(
                     array(
                         "fieldName"=>"someField",
                         "property"=>array("some:property"),
                     )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    public function testConfigWithoutDefaultNamespaceThrowsException()
    {
        $this->setExpectedException(
            'MongoTripodConfigException',
            'Mandatory config key [defaultContext] is missing from config');
        $config = array();
        $config["data_sources"] = array(
            "db"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source"=>"db",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config["stores"]["tripod_php_testing"]["view_specifications"] = array(
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
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"db");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
    }

    /**
     * the indexesGroupedByCollection method should not only return each of the indexes that are defined explicitly in the config.json,
     * but also include indexes that are inserted by MongoTripodConfig object because they are needed by tripod
     */
    public function testGetIndexesGroupedByCollection()
    {
        $indexSpecs = MongoTripodConfig::getInstance()->getIndexesGroupedByCollection("tripod_php_testing");

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

        $this->assertEquals(array("value.isbn"=>1), $indexSpecs[TABLE_ROWS_COLLECTION]["rs1"][0]);
        $this->assertEquals(array("value._graphs.sioc:has_container.u"=>1,"value._graphs.sioc:topic.u"=>1), $indexSpecs[VIEWS_COLLECTION]["rs1"][0]);
    }

    public function testGetReplicaSetName()
    {
        $config = array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "rs1"=>array(
                "type"=>"mongo",
                "replicaSet"=>"myreplicaset",
                "connection"=>"sometestval",
            ),
            "mongo1"=>array(
                "type"=>"mongo",
                "connection"=>"sometestval",
            ),
            "tlog"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://abc:zyx@localhost:27018"
            ),
            "queue"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log", "data_source"=>"tlog");
        $config["stores"] = array(
            "tripod_php_testing"=>array(
                "data_source" => "rs1",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            ),
            "testing_2"=>array(
                "data_source" => "mongo1",
                "pods"=>array(
                    "CBD_testing"=>array(
                    )
                )
            )
        );
        $config['queue'] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"queue");
        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
        $this->assertEquals("myreplicaset", $mtc->getReplicaSetName($mtc->getDefaultDataSourceForStore("tripod_php_testing")));

        $this->assertNull($mtc->getReplicaSetName("testing_2"));
    }

    public function testGetViewSpecification(){
        $expectedVspec = array(
            "_id"=> "v_resource_full",
            "_version" => "0.1",
            "from"=>"CBD_testing",
            "to_data_source"=>"rs1", // This should get added automatically
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

        $vspec = MongoTripodConfig::getInstance()->getViewSpecification('tripod_php_testing', "v_resource_full");
        $this->assertEquals($expectedVspec, $vspec);

        $vspec = MongoTripodConfig::getInstance()->getViewSpecification('tripod_php_testing', "doesnt_exist");
        $this->assertNull($vspec);
    }

    public function testGetTableSpecification()
    {
        $expectedTspec = array(
            "_id"=>"t_resource",
            "type"=>"acorn:Resource",
            "from"=>"CBD_testing",
            "to_data_source"=>"rs1", // This should be added automatically
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

        $tspec = MongoTripodConfig::getInstance()->getTableSpecification("tripod_php_testing", "t_resource");
        $this->assertEquals($expectedTspec, $tspec);

        $tspec = MongoTripodConfig::getInstance()->getTableSpecification("tripod_php_testing", "doesnt_exist");
        $this->assertNull($tspec);
    }


    public function testSearchConfigNotPresent()
    {
        $config=array();
        $config["defaultContext"] = "http://talisaspire.com/";
        $config["data_sources"] = array(
            "mongo1"=>array(
                "type"=>"mongo",
                "connection"=>"mongodb://localhost"
            )
        );
        $config["stores"] = array(
            "tripod_php_testing" => array(
                "data_source"=>"mongo1",
                "pods" => array(
                    "CBD_testing" => array()
                )
            )
        );
        $config["transaction_log"] = array("database"=>"transactions","collection"=>"transaction_log","data_source"=>"mongo1");
        $config['queue'] = array(
            "database"=>"testing_queue",
            "collection"=>"q_queue",
            "data_source"=>"mongo1"
        );

        MongoTripodConfig::setConfig($config);
        $mtc = MongoTripodConfig::getInstance();
        $this->assertNull($mtc->getSearchProviderClassName("tripod_php_testing"));
        $this->assertEquals(array(), $mtc->getSearchDocumentSpecifications("tripod_php_testing"));
    }

    public function testGetAllTypesInSpecifications()
    {
        $types = $this->tripodConfig->getAllTypesInSpecifications("tripod_php_testing");
        $this->assertEquals(9, count($types), "There should be 9 types based on the configured view, table and search specifications in config.json");
        $expectedValues = array(
            "acorn:Resource",
            "acorn:Work",
            "http://talisaspire.com/schema#Work2",
            "acorn:Work2",
            "bibo:Book",
            "resourcelist:List",
            "spec:User",
            "bibo:Document",
            "baseData:Wibble"
        );

        foreach($expectedValues as $expected){
            $this->assertContains($expected, $types, "List of types should have contained $expected");
        }
    }

    public function testGetPredicatesForTableSpec()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec("tripod_php_testing", 't_users');
        $this->assertEquals(6, count($predicates), "There should be 6 predicates defined in t_users in config.json");
        $expectedValues = array(
            'rdf:type',
            'foaf:firstName',
            'foaf:surname',
            'temp:last_login',
            'temp:last_login_invalid',
            'temp:last_login_DOES_NOT_EXIST'
        );

        foreach($expectedValues as $expected){
            $this->assertContains($expected, $predicates, "List of predicates should have contained $expected");
        }
    }

    public function testGetPredicatesForSearchDocSpec()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec("tripod_php_testing", 'i_search_list');
        $this->assertEquals(6, count($predicates), "There should be 6 predicates defined in i_search_list in config.json");

        $expectedValues = array(
            'rdf:type',
            'spec:name',
            'resourcelist:description',
            'resourcelist:usedBy', // defined in the join
            'aiiso:name',
            'aiiso:code'
        );

        foreach($expectedValues as $expected){
            $this->assertContains($expected, $predicates, "List of predicates should have contained $expected");
        }
    }

    public function testGetPredicatesForSpecFilter()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 'i_search_filter_parse');

        $this->assertEquals(6, count($predicates), "There should be 6 predicates defined in i_search_filter_parse in config.json");

        $expectedValues = array(
            'rdf:type',
            'spec:name',
            'dct:title',
            'dct:created', // defined only in the filter
            'temp:numberOfThings', // defined only in the filter
            'temp:amountOfTimeSpent' // defined only in the filter
        );

        foreach($expectedValues as $expected){
            $this->assertContains($expected, $predicates, "List of predicates should have contained $expected");
        }
    }

    public function testCollectionReadPreferencesAreAppliedToDatabase()
    {
        /** @var PHPUnit_Framework_MockObject_MockObject|MongoTripodTestConfig $mockConfig */
        $mockConfig = $this->getMock('MongoTripodTestConfig', array('getDatabase'));
        $mockConfig->loadConfig(json_decode(file_get_contents(dirname(__FILE__).'/data/config.json'), true));
        $mockConfig->expects($this->exactly(2))
            ->method('getDatabase')
            ->withConsecutive(
                  array('tripod_php_testing', 'rs1', MongoClient::RP_SECONDARY_PREFERRED),
                  array('tripod_php_testing', 'rs1', MongoClient::RP_NEAREST)
            )
            ->will($this->returnCallback(
                function()
                {
                    $mongo = new MongoClient();
                    return $mongo->selectDB('tripod_php_testing');
                }
            ));

        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', MongoClient::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', MongoClient::RP_NEAREST);

    }

    public function testDataLoadedInConfiguredDataSource()
    {
        $storeName = 'tripod_php_testing';

        $dataSourcesForStore = array();
        $config = MongoTripodConfig::getInstance();
        $pods = $config->getPods($storeName);

        foreach($pods as $pod)
        {
            if(!in_array($config->getDataSourceForPod($storeName, $pod), $dataSourcesForStore))
            {
                $dataSourcesForStore[] = $config->getDataSourceForPod($storeName, $pod);
            }
        }

        foreach($config->getViewSpecifications($storeName) as $id=>$spec)
        {
            if(!in_array($spec['to_data_source'], $dataSourcesForStore))
            {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach($config->getTableSpecifications($storeName) as $id=>$spec)
        {
            if(!in_array($spec['to_data_source'], $dataSourcesForStore))
            {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach($config->getSearchDocumentSpecifications($storeName) as $id=>$spec)
        {
            if(!in_array($spec['to_data_source'], $dataSourcesForStore))
            {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        if(count($dataSourcesForStore) < 2)
        {
            $this->markTestSkipped("Less than two datasources configured for store, nothing to test");
        }

        $diff = false;

        $cfg = MongoTripodConfig::getConfig();
        $defaultDataSource = $cfg["data_sources"][$config->getDefaultDataSourceForStore($storeName)];

        foreach($dataSourcesForStore as $source)
        {
            if($cfg['data_sources'][$source] != $defaultDataSource)
            {
                $diff = true;
                break;
            }
            $config->getDatabase($storeName, $source)->drop();
        }

        if($diff == false)
        {
            $this->markTestSkipped("All datasources configured for store use same configuration, nothing to test");
        }

        $this->tripod = new MongoTripod('CBD_testing', $storeName, array(OP_ASYNC=>array(OP_VIEWS=>true,OP_TABLES=>false,OP_SEARCH=>false)));
        $this->loadBaseDataViaTripod();

        $graph = new MongoGraph();
        $subject = 'http://example.com/' . uniqid();
        $labeller = new MongoTripodLabeller();
        $graph->add_resource_triple($subject, RDF_TYPE, $labeller->qname_to_uri('foaf:Person'));
        $graph->add_literal_triple($subject, FOAF_NAME, "Anne Example");
        $this->tripod->saveChanges(new ExtendedGraph(), $graph);

        $newGraph = $this->tripod->describeResource($subject);
        $newGraph->add_literal_triple($subject, $labeller->qname_to_uri('foaf:email'), 'anne@example.com');
        $this->tripod->saveChanges($graph, $newGraph);

        // Add an item to the queue
        $queue = new MongoTripodQueue();
        $item = new ModifiedSubject(array('collection'=>'CBD_wibble', 'database'=>'foo'));
        $queue->addItem($item);

        // Generate views and tables
        foreach($config->getViewSpecifications($storeName) as $viewId=>$viewSpec)
        {
            $this->tripod->getTripodViews()->generateView($viewId);
        }
        foreach($config->getTableSpecifications($storeName) as $tableId=>$tableSpec)
        {
            $this->tripod->generateTableRows($tableId);
        }

        // Create some locks so we have a collection
        $lCollection = $config->getCollectionForLocks($storeName);
        $lCollection->drop();
        $lCollection->insert(array(_ID_KEY=>array(_ID_RESOURCE=>'foo',_ID_CONTEXT=>'bar'), _LOCKED_FOR_TRANS=>'foobar'));
        $lCollection->insert(array(_ID_KEY=>array(_ID_RESOURCE=>'baz',_ID_CONTEXT=>'bar'), _LOCKED_FOR_TRANS=>'wibble'));
        $this->tripod->removeInertLocks('foobar', 'reason1');

        $collectionsForDataSource = array();
        $collectionsForDataSource['rs1'] = array(
            VIEWS_COLLECTION, SEARCH_INDEX_COLLECTION, TABLE_ROWS_COLLECTION, 'CBD_testing',
            AUDIT_MANUAL_ROLLBACKS_COLLECTION, LOCKS_COLLECTION, 'q_queue'
        );

        $collectionsForDataSource['rs2'] = array(VIEWS_COLLECTION, SEARCH_INDEX_COLLECTION, TABLE_ROWS_COLLECTION, 'CBD_testing_2', 'transaction_log');
        $specs = array();
        $specs['views'] = MongoTripodConfig::getInstance()->getViewSpecifications($storeName);
        $specs['search'] = MongoTripodConfig::getInstance()->getSearchDocumentSpecifications($storeName);
        $specs['table_rows'] = MongoTripodConfig::getInstance()->getTableSpecifications($storeName);
        $specsForDataSource = array();

        foreach(array('views', 'search', 'table_rows') as $type)
        {
            foreach($specs[$type] as $spec)
            {
                if(!isset($specsForDataSource[$spec['to_data_source']][$type]))
                {
                    $specsForDataSource[$spec['to_data_source']][$type] = array();
                }
                $specsForDataSource[$spec['to_data_source']][$type][] = $spec['_id'];
            }
        }


        $foundCollections = array();

        foreach($dataSourcesForStore  as $source)
        {
            /** @var MongoCollection $collection */
            foreach($config->getDatabase($storeName, $source)->listCollections() as $collection)
            {
                $name = $collection->getName();
                $foundCollections[] = $name;
                $this->assertContains($name, $collectionsForDataSource[$source], "Source " . $source . " does not include " . $name);
                switch($name)
                {
                    case 'views':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['views']));

                        $this->assertGreaterThan(0, $collection->count(array()), "views collection did not have at least 1 document in data source " . $source);
                        foreach($dataSourcesForStore as $otherSource)
                        {
                            if($otherSource == $source)
                            {
                                continue;
                            }
                            foreach($specsForDataSource[$otherSource]['views'] as $view)
                            {
                                $this->assertEquals(0, $collection->count(array('_id.type'=>$view)), $view . " had at least 1 document in data source " . $source);
                            }
                        }

                        break;
                    case 'search':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['search']));

                        $this->assertGreaterThan(0, $collection->count(array()), "search collection did not have at least 1 document in data source " . $source);

                        foreach($dataSourcesForStore as $otherSource)
                        {
                            if($otherSource == $source)
                            {
                                continue;
                            }
                            foreach($specsForDataSource[$otherSource]['search'] as $search)
                            {
                                $this->assertEquals(0, $collection->count(array('_id.type'=>$search)), $search . " had at least 1 document in data source " . $source);
                            }
                        }
                        break;
                    case 'table_rows':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['table_rows']));

                        $this->assertGreaterThan(0, $collection->count(array()), "table_rows collection did not have at least 1 document in data source " . $source);
                        foreach($dataSourcesForStore as $otherSource)
                        {
                            if($otherSource == $source)
                            {
                                continue;
                            }
                            foreach($specsForDataSource[$otherSource]['table_rows'] as $t)
                            {
                                $this->assertEquals(0, $collection->count(array('_id.type'=>$t)), $t . " had at least 1 document in data source " . $source);
                            }
                        }
                        break;
                    case 'CBD_testing':
                        $this->assertGreaterThan(0, $collection->count(array()), "CBD_testing collection did not have at least 1 document in data source " . $source);
                        break;
                    case 'CBD_testing_2':
                        $this->assertGreaterThan(0, $collection->count(array()), "CBD_testing_2 collection did not have at least 1 document in data source " . $source);
                        break;
                }

            }
        }
    }
}