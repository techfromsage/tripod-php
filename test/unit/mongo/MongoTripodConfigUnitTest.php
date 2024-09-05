<?php

use MongoDB\Driver\ReadPreference;
use MongoDB\Client;
use MongoDB\Driver\Exception\ConnectionTimeoutException;

class MongoTripodConfigUnitTest extends MongoTripodTestBase
{
    /**
     * @var Tripod\Mongo\Config
     */
    private $tripodConfig;

    protected function setUp(): void
    {
        parent::setup();
        $this->tripodConfig = Tripod\Config::getInstance();
    }

    public function testGetInstanceThrowsExceptionIfSetInstanceNotCalledFirst()
    {
        // to test that the instance throws an exception if it is called before calling setConfig
        // i first have to destroy the instance that is created in the setUp() method of our test suite.

        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Call Config::setConfig() first');
        unset($this->tripodConfig);

        Tripod\Config::destroy();
        Tripod\Config::getInstance();
    }

    public function testNamespaces()
    {
        $ns = $this->tripodConfig->getNamespaces();
        $this->assertEquals(16, count($ns), 'Incorrect number of namespaces');

        $expectedNs = [];

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
        $this->assertEquals($expectedNs, $ns, 'Incorrect namespaces');
    }

    public function testTConfig()
    {
        $config = Tripod\Config::getInstance();
        $cfg = Tripod\Config::getConfig();
        $tConfig = $config->getTransactionLogConfig();
        $this->assertEquals('tripod_php_testing', $tConfig['database']);
        $this->assertEquals('transaction_log', $tConfig['collection']);
    }

    public function testCardinality()
    {
        $cardinality = $this->tripodConfig->getCardinality('tripod_php_testing', 'CBD_testing', 'dct:created');
        $this->assertEquals(1, $cardinality, 'Expected cardinality of 1 for dct:created');

        $cardinality = $this->tripodConfig->getCardinality('tripod_php_testing', 'CBD_testing', 'random:property');
        $this->assertEquals(-1, $cardinality, 'Expected cardinality of 1 for random:property');
    }

    public function testCompoundIndexAllArraysThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Compound index IllegalCompoundIndex has more than one field with cardinality > 1 - mongo will not be able to build this index');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db1' => [
                'type' => 'mongo',
                'connection' => 'mongodb://mongodb',
            ],
            'db2' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db1'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db2',
                'pods' => [
                    'CBD_testing' => [
                        'indexes' => [
                            'IllegalCompoundIndex' => [
                                'rdf:type.value' => 1,
                                'dct:subject.value' => 1],
                        ],
                    ],
                ],
            ],
        ];

        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testSearchConfig()
    {
        $config = Tripod\Config::getInstance();
        $this->assertEquals('\Tripod\Mongo\MongoSearchProvider', $config->getSearchProviderClassName('tripod_php_testing'));

        $this->assertEquals(3, count($config->getSearchDocumentSpecifications('tripod_php_testing')));
    }

    public function testCardinalityRuleWithNoNamespace()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Cardinality 'foo:bar' does not have the namespace defined");

        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                        'cardinality' => [
                            'foo:bar' => 1,
                        ],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testGetSearchDocumentSpecificationsByType()
    {
        $expectedSpec = [
            [
                '_id' => 'i_search_list',
                'type' => ['resourcelist:List'],
                'from' => 'CBD_testing',
                'to_data_source' => 'rs1', // This should be added automatically
                'filter' => [
                    ['condition' => [
                        'spec:name.l' => ['$exists' => true],
                    ]],
                ],
                'indices' => [
                    [
                        'fieldName' => 'search_terms',
                        'predicates' => ['spec:name', 'resourcelist:description'],
                    ],
                ],
                'fields' => [
                    [
                        'fieldName' => 'result.title',
                        'predicates' => ['spec:name'],
                        'limit' => 1,
                    ],
                    [
                        'fieldName' => 'result.link',
                        'value' => 'link',
                    ],
                ],
                'joins' => [
                    'resourcelist:usedBy' => [
                        'indices' => [
                            [
                                'fieldName' => 'search_terms',
                                'predicates' => ['aiiso:name', 'aiiso:code'],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        $actualSpec = Tripod\Config::getInstance()->getSearchDocumentSpecifications('tripod_php_testing', 'resourcelist:List');
        $this->assertEquals($expectedSpec, $actualSpec);
    }

    public function testGetSearchDocumentSpecificationsById()
    {
        $expectedSpec
            = [
                '_id' => 'i_search_list',
                'type' => ['resourcelist:List'],
                'from' => 'CBD_testing',
                'to_data_source' => 'rs1', // this is added automatically
                'filter' => [
                    ['condition' => [
                        'spec:name.l' => ['$exists' => true],
                    ]],
                ],
                'indices' => [
                    [
                        'fieldName' => 'search_terms',
                        'predicates' => ['spec:name', 'resourcelist:description'],
                    ],
                ],
                'fields' => [
                    [
                        'fieldName' => 'result.title',
                        'predicates' => ['spec:name'],
                        'limit' => 1,
                    ],
                    [
                        'fieldName' => 'result.link',
                        'value' => 'link',
                    ],
                ],
                'joins' => [
                    'resourcelist:usedBy' => [
                        'indices' => [
                            [
                                'fieldName' => 'search_terms',
                                'predicates' => ['aiiso:name', 'aiiso:code'],
                            ],
                        ],
                    ],
                ],
            ];
        $actualSpec = Tripod\Config::getInstance()->getSearchDocumentSpecification('tripod_php_testing', 'i_search_list');
        $this->assertEquals($expectedSpec, $actualSpec);
    }

    public function testGetSearchDocumentSpecificationsWhereNoneExists()
    {
        $expectedSpec = [];
        $actualSpec = Tripod\Config::getInstance()->getSearchDocumentSpecifications('something:doesntexist');
        $this->assertEquals($expectedSpec, $actualSpec);
    }

    public function testViewSpecCountWithoutTTLThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Aggregate function counts exists in spec, but no TTL defined');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
        ];
        $config['stores']['tripod_php_testing']['view_specifications'] = [
            [
                '_id' => 'v_illegal_counts',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'counts' => [
                    'acorn:resourceCount' => [
                        'filter' => ['rdf:type.value' => 'http://talisaspire.com/schema#Resource'],
                        'property' => 'dct:isVersionOf',
                    ],
                ],
                'joins' => ['dct:hasVersion' => []],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testViewSpecCountNestedInJoinWithoutTTLThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Aggregate function counts exists in spec, but no TTL defined');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
        ];
        $config['stores']['tripod_php_testing']['view_specifications'] = [
            [
                '_id' => 'v_illegal_counts',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'joins' => [
                    'acorn:seeAlso' => [
                        'counts' => [
                            'acorn:resourceCount' => [
                                'filter' => ['rdf:type.value' => 'http://talisaspire.com/schema#Resource'],
                                'property' => 'dct:isVersionOf',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecNestedCountWithoutPropertyThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Count spec does not contain property');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_counts',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'joins' => [
                    'acorn:resourceCount' => [
                        'filter' => ['rdf:type.value' => 'http://talisaspire.com/schema#Resource'],
                        'property' => 'dct:isVersionOf',
                        'counts' => [['fieldName' => 'someField']],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecNested2ndLevelCountWithoutFieldNameThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Count spec does not contain fieldName');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_counts',
                'from' => 'CBD_testing',
                'type' => 'http://talisaspire.com/schema#Work',
                'joins' => [
                    'acorn:resourceCount' => [
                        'filter' => ['rdf:type.value' => 'http://talisaspire.com/schema#Resource'],
                        'property' => 'dct:isVersionOf',
                        'joins' => [
                            'another:property' => [
                                'counts' => [['property' => 'value']],
                            ],
                        ],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecFieldWithoutFieldName()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Field spec does not contain fieldName');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_spec',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'fields' => [
                    [
                        'predicates' => ['rdf:type'],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecFieldWithoutPredicates()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Field spec does not contain predicates');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_spec',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'fields' => [
                    [
                        'fieldName' => 'some_field',
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecCountWithoutProperty()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Count spec does not contain property');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_spec',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'counts' => [
                    [
                        'fieldName' => 'some_field',
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecCountWithoutFieldName()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Count spec does not contain fieldName');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_spec',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'counts' => [
                    [
                        'property' => 'some:property',
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testTableSpecCountWithoutPropertyAsAString()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Count spec property was not a string');
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];

        $config['stores']['tripod_php_testing']['table_specifications'] = [
            [
                '_id' => 't_illegal_spec',
                'type' => 'http://talisaspire.com/schema#Work',
                'from' => 'CBD_testing',
                'counts' => [
                    [
                        'fieldName' => 'someField',
                        'property' => ['some:property'],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    public function testConfigWithoutDefaultNamespaceThrowsException()
    {
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Mandatory config key [defaultContext] is missing from config');
        $config = [];
        $config['data_sources'] = [
            'db' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'db'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'db',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
        ];
        $config['stores']['tripod_php_testing']['view_specifications'] = [
            [
                '_id' => 'v_illegal_counts',
                'type' => 'http://talisaspire.com/schema#Work',
                'joins' => [
                    'acorn:seeAlso' => [
                        'counts' => [
                            'acorn:resourceCount' => [
                                'filter' => ['rdf:type.value' => 'http://talisaspire.com/schema#Resource'],
                                'property' => 'dct:isVersionOf',
                            ],
                        ],
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
    }

    /**
     * the indexesGroupedByCollection method should not only return each of the indexes that are defined explicitly in the config.json,
     * but also include indexes that are inserted by Config object because they are needed by tripod
     */
    public function testGetIndexesGroupedByCollection()
    {
        $indexSpecs = Tripod\Config::getInstance()->getIndexesGroupedByCollection('tripod_php_testing');

        $this->assertArrayHasKey('CBD_testing', $indexSpecs);
        $this->assertArrayHasKey('index1', $indexSpecs['CBD_testing']);
        $this->assertArrayHasKey('dct:subject.u', $indexSpecs['CBD_testing']['index1']);
        $this->assertArrayHasKey('index2', $indexSpecs['CBD_testing']);
        $this->assertArrayHasKey('rdf:type.u', $indexSpecs['CBD_testing_2']['index1']);

        $this->assertArrayHasKey(_LOCKED_FOR_TRANS_INDEX, $indexSpecs['CBD_testing']);
        $this->assertArrayHasKey('_id', $indexSpecs['CBD_testing'][_LOCKED_FOR_TRANS_INDEX]);
        $this->assertArrayHasKey(_LOCKED_FOR_TRANS, $indexSpecs['CBD_testing'][_LOCKED_FOR_TRANS_INDEX]);

        $this->assertArrayHasKey('CBD_testing_2', $indexSpecs);
        $this->assertArrayHasKey('index1', $indexSpecs['CBD_testing']);
        $this->assertArrayHasKey('rdf:type.u', $indexSpecs['CBD_testing_2']['index1']);

        $this->assertArrayHasKey(_LOCKED_FOR_TRANS_INDEX, $indexSpecs['CBD_testing_2']);
        $this->assertArrayHasKey('_id', $indexSpecs['CBD_testing_2'][_LOCKED_FOR_TRANS_INDEX]);
        $this->assertArrayHasKey(_LOCKED_FOR_TRANS, $indexSpecs['CBD_testing_2'][_LOCKED_FOR_TRANS_INDEX]);

        $this->assertEquals(['value.isbn' => 1], $indexSpecs[TABLE_ROWS_COLLECTION]['rs1'][0]);
        $this->assertEquals(['value._graphs.sioc:has_container.u' => 1, 'value._graphs.sioc:topic.u' => 1], $indexSpecs[VIEWS_COLLECTION]['rs1'][0]);
    }

    public function testGetReplicaSetName()
    {
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'rs1' => [
                'type' => 'mongo',
                'replicaSet' => 'myreplicaset',
                'connection' => 'sometestval',
            ],
            'mongo1' => [
                'type' => 'mongo',
                'connection' => 'sometestval',
            ],
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://abc:zyx@localhost:27018',
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'tlog'];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'rs1',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
            'testing_2' => [
                'data_source' => 'mongo1',
                'pods' => [
                    'CBD_testing' => [
                    ],
                ],
            ],
        ];
        Tripod\Config::setConfig($config);
        /** @var Tripod\Mongo\Config */
        $mtc = Tripod\Config::getInstance();
        $this->assertEquals('myreplicaset', $mtc->getReplicaSetName($mtc->getDefaultDataSourceForStore('tripod_php_testing')));

        $this->assertNull($mtc->getReplicaSetName('testing_2'));
    }

    public function testGetReplicaSetNameFromConnectionString()
    {
        Tripod\Config::setConfig([
            'defaultContext' => 'http://talisaspire.com/',
            'data_sources' => [
                'rs1' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://a.foo.com,b.foo.com/?replicaSet=myReplicaSet&authSource=admin',
                ],
                'rs2' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://c.foo.com,d.foo.com/?replicaSet=',
                ],
            ],
            'transaction_log' => ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'rs1'],
            'stores' => [],
        ]);
        /** @var Tripod\Mongo\Config */
        $mtc = Tripod\Config::getInstance();
        $this->assertEquals('myReplicaSet', $mtc->getReplicaSetName('rs1'));
        $this->assertEquals(null, $mtc->getReplicaSetName('rs2'));
    }

    public function testGetViewSpecification()
    {
        $expectedVspec = [
            '_id' => 'v_resource_full',
            '_version' => '0.1',
            'from' => 'CBD_testing',
            'to_data_source' => 'rs1', // This should get added automatically
            'ensureIndexes' => [
                [
                    'value._graphs.sioc:has_container.u' => 1,
                    'value._graphs.sioc:topic.u' => 1,
                ],
            ],
            'type' => 'acorn:Resource',
            'include' => ['rdf:type', 'searchterms:topic'],
            'joins' => [
                'dct:isVersionOf' => [
                    'include' => [
                        'dct:subject',
                        'rdf:type',
                    ],
                ],
            ],
        ];

        $vspec = Tripod\Config::getInstance()->getViewSpecification('tripod_php_testing', 'v_resource_full');
        $this->assertEquals($expectedVspec, $vspec);

        $vspec = Tripod\Config::getInstance()->getViewSpecification('tripod_php_testing', 'doesnt_exist');
        $this->assertNull($vspec);
    }

    public function testGetTableSpecification()
    {
        $expectedTspec = [
            '_id' => 't_resource',
            'type' => 'acorn:Resource',
            'from' => 'CBD_testing',
            'to_data_source' => 'rs1', // This should be added automatically
            'ensureIndexes' => [['value.isbn' => 1]],
            'fields' => [
                [
                    'fieldName' => 'type',
                    'predicates' => ['rdf:type'],
                ],
                [
                    'fieldName' => 'isbn',
                    'predicates' => ['bibo:isbn13'],
                ],
            ],
            'joins' => [
                'dct:isVersionOf' => [
                    'fields' => [
                        [
                            'fieldName' => 'isbn13',
                            'predicates' => ['bibo:isbn13'],
                        ],
                    ],
                ],
            ],
        ];

        $tspec = Tripod\Config::getInstance()->getTableSpecification('tripod_php_testing', 't_resource');
        $this->assertEquals($expectedTspec, $tspec);

        $tspec = Tripod\Config::getInstance()->getTableSpecification('tripod_php_testing', 'doesnt_exist');
        $this->assertNull($tspec);
    }

    public function testSearchConfigNotPresent()
    {
        $config = [];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['data_sources'] = [
            'mongo1' => [
                'type' => 'mongo',
                'connection' => 'mongodb://mongodb',
            ],
        ];
        $config['stores'] = [
            'tripod_php_testing' => [
                'data_source' => 'mongo1',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];
        $config['transaction_log'] = ['database' => 'transactions', 'collection' => 'transaction_log', 'data_source' => 'mongo1'];

        Tripod\Config::setConfig($config);
        $mtc = Tripod\Config::getInstance();
        $this->assertNull($mtc->getSearchProviderClassName('tripod_php_testing'));
        $this->assertEquals([], $mtc->getSearchDocumentSpecifications('tripod_php_testing'));
    }

    public function testGetAllTypesInSpecifications()
    {
        $types = $this->tripodConfig->getAllTypesInSpecifications('tripod_php_testing');
        $this->assertEquals(
            12,
            count($types),
            'There should be 12 types based on the configured view, table and search specifications in config.json'
        );
        $expectedValues = [
            'acorn:Resource',
            'acorn:ResourceForTruncating',
            'acorn:Work',
            'http://talisaspire.com/schema#Work2',
            'acorn:Work2',
            'bibo:Book',
            'resourcelist:List',
            'spec:User',
            'bibo:Document',
            'baseData:Wibble',
            'baseData:DocWithSequence',
            'dctype:Event',
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $types, "List of types should have contained {$expected}");
        }
    }

    public function testGetPredicatesForTableSpec()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 't_users');
        $this->assertEquals(6, count($predicates), 'There should be 6 predicates defined in t_users in config.json');
        $expectedValues = [
            'rdf:type',
            'foaf:firstName',
            'foaf:surname',
            'temp:last_login',
            'temp:last_login_invalid',
            'temp:last_login_DOES_NOT_EXIST',
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, "List of predicates should have contained {$expected}");
        }
    }

    public function testGetPredicatesForSearchDocSpec()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 'i_search_list');
        $this->assertEquals(6, count($predicates), 'There should be 6 predicates defined in i_search_list in config.json');

        $expectedValues = [
            'rdf:type',
            'spec:name',
            'resourcelist:description',
            'resourcelist:usedBy', // defined in the join
            'aiiso:name',
            'aiiso:code',
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, "List of predicates should have contained {$expected}");
        }
    }

    public function testGetPredicatesForSpecFilter()
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 'i_search_filter_parse');

        $this->assertEquals(6, count($predicates), 'There should be 6 predicates defined in i_search_filter_parse in config.json');

        $expectedValues = [
            'rdf:type',
            'spec:name',
            'dct:title',
            'dct:created', // defined only in the filter
            'temp:numberOfThings', // defined only in the filter
            'temp:amountOfTimeSpent', // defined only in the filter
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, "List of predicates should have contained {$expected}");
        }
    }

    public function testCollectionReadPreferencesAreAppliedToDatabase()
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getDatabase'])
            ->getMock();
        $mockConfig->loadConfig(json_decode(file_get_contents(dirname(__FILE__) . '/data/config.json'), true));
        $mockConfig->expects($this->exactly(2))
            ->method('getDatabase')
            ->withConsecutive(
                ['tripod_php_testing', 'rs1', ReadPreference::RP_SECONDARY_PREFERRED],
                ['tripod_php_testing', 'rs1', ReadPreference::RP_NEAREST]
            )
            ->will($this->returnCallback(
                function () {
                    $mongo = new Client();
                    return $mongo->selectDatabase('tripod_php_testing');
                }
            ));

        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_NEAREST);

    }

    public function testDataLoadedInConfiguredDataSource()
    {
        $storeName = 'tripod_php_testing';

        $dataSourcesForStore = [];
        /** @var Tripod\Mongo\Config */
        $config = Tripod\Config::getInstance();
        $pods = $config->getPods($storeName);

        foreach ($pods as $pod) {
            if (!in_array($config->getDataSourceForPod($storeName, $pod), $dataSourcesForStore)) {
                $dataSourcesForStore[] = $config->getDataSourceForPod($storeName, $pod);
            }
        }

        foreach ($config->getViewSpecifications($storeName) as $id => $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach ($config->getTableSpecifications($storeName) as $id => $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach ($config->getSearchDocumentSpecifications($storeName) as $id => $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        if (count($dataSourcesForStore) < 2) {
            $this->markTestSkipped('Less than two datasources configured for store, nothing to test');
        }

        $diff = false;

        $cfg = Tripod\Config::getConfig();
        $defaultDataSource = $cfg['data_sources'][$config->getDefaultDataSourceForStore($storeName)];

        foreach ($dataSourcesForStore as $source) {
            if ($cfg['data_sources'][$source] != $defaultDataSource) {
                $diff = true;
                break;
            }
            $config->getDatabase($storeName, $source)->drop();
        }

        if ($diff == false) {
            $this->markTestSkipped('All datasources configured for store use same configuration, nothing to test');
        }

        $this->tripod = new Tripod\Mongo\Driver('CBD_testing', $storeName, [OP_ASYNC => [OP_VIEWS => true, OP_TABLES => false, OP_SEARCH => false]]);
        $this->loadResourceDataViaTripod();

        $graph = new Tripod\Mongo\MongoGraph();
        $subject = 'http://example.com/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $graph->add_resource_triple($subject, RDF_TYPE, $labeller->qname_to_uri('foaf:Person'));
        $graph->add_literal_triple($subject, FOAF_NAME, 'Anne Example');
        $this->tripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $newGraph = $this->tripod->describeResource($subject);
        $newGraph->add_literal_triple($subject, $labeller->qname_to_uri('foaf:email'), 'anne@example.com');
        $this->tripod->saveChanges($graph, $newGraph);

        // Generate views and tables
        foreach ($config->getViewSpecifications($storeName) as $viewId => $viewSpec) {
            $this->tripod->getTripodViews()->generateView($viewId);
        }
        foreach ($config->getTableSpecifications($storeName) as $tableId => $tableSpec) {
            $this->tripod->generateTableRows($tableId);
        }

        // Create some locks so we have a collection
        $lCollection = $config->getCollectionForLocks($storeName);
        $lCollection->drop();
        $lCollection->insertOne([_ID_KEY => [_ID_RESOURCE => 'foo', _ID_CONTEXT => 'bar'], _LOCKED_FOR_TRANS => 'foobar']);
        $lCollection->insertOne([_ID_KEY => [_ID_RESOURCE => 'baz', _ID_CONTEXT => 'bar'], _LOCKED_FOR_TRANS => 'wibble']);
        $this->tripod->removeInertLocks('foobar', 'reason1');

        $collectionsForDataSource = [];
        $collectionsForDataSource['rs1'] = [
            VIEWS_COLLECTION, SEARCH_INDEX_COLLECTION, TABLE_ROWS_COLLECTION, 'CBD_testing',
            AUDIT_MANUAL_ROLLBACKS_COLLECTION, LOCKS_COLLECTION,
        ];

        $collectionsForDataSource['rs2'] = [VIEWS_COLLECTION, SEARCH_INDEX_COLLECTION, TABLE_ROWS_COLLECTION, 'CBD_testing_2', 'transaction_log'];
        $specs = [];
        $specs['views'] = Tripod\Config::getInstance()->getViewSpecifications($storeName);
        $specs['search'] = Tripod\Config::getInstance()->getSearchDocumentSpecifications($storeName);
        $specs['table_rows'] = Tripod\Config::getInstance()->getTableSpecifications($storeName);
        $specsForDataSource = [];

        foreach (['views', 'search', 'table_rows'] as $type) {
            foreach ($specs[$type] as $spec) {
                if (!isset($specsForDataSource[$spec['to_data_source']][$type])) {
                    $specsForDataSource[$spec['to_data_source']][$type] = [];
                }
                $specsForDataSource[$spec['to_data_source']][$type][] = $spec['_id'];
            }
        }

        $foundCollections = [];

        foreach ($dataSourcesForStore as $source) {
            $db = $config->getDatabase($storeName, $source);
            foreach ($db->listCollections() as $collectionInfo) {
                $name = $collectionInfo->getName();
                $collection = $db->selectCollection($name);
                $foundCollections[] = $name;
                $this->assertContains($name, $collectionsForDataSource[$source], 'Source ' . $source . ' does not include ' . $name);
                switch ($name) {
                    case 'views':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['views']));

                        $this->assertGreaterThan(0, $collection->count([]), 'views collection did not have at least 1 document in data source ' . $source);
                        foreach ($dataSourcesForStore as $otherSource) {
                            if ($otherSource == $source) {
                                continue;
                            }
                            foreach ($specsForDataSource[$otherSource]['views'] as $view) {
                                $this->assertEquals(0, $collection->count(['_id.type' => $view]), $view . ' had at least 1 document in data source ' . $source);
                            }
                        }

                        break;
                    case 'search':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['search']));

                        $this->assertGreaterThan(0, $collection->count([]), 'search collection did not have at least 1 document in data source ' . $source);

                        foreach ($dataSourcesForStore as $otherSource) {
                            if ($otherSource == $source) {
                                continue;
                            }
                            foreach ($specsForDataSource[$otherSource]['search'] as $search) {
                                $this->assertEquals(0, $collection->count(['_id.type' => $search]), $search . ' had at least 1 document in data source ' . $source);
                            }
                        }
                        break;
                    case 'table_rows':
                        $this->assertGreaterThan(0, count($specsForDataSource[$source]['table_rows']));

                        $this->assertGreaterThan(0, $collection->count([]), 'table_rows collection did not have at least 1 document in data source ' . $source);
                        foreach ($dataSourcesForStore as $otherSource) {
                            if ($otherSource == $source) {
                                continue;
                            }
                            foreach ($specsForDataSource[$otherSource]['table_rows'] as $t) {
                                $this->assertEquals(0, $collection->count(['_id.type' => $t]), $t . ' had at least 1 document in data source ' . $source);
                            }
                        }
                        break;
                    case 'CBD_testing':
                        $this->assertGreaterThan(0, $collection->count([]), 'CBD_testing collection did not have at least 1 document in data source ' . $source);
                        break;
                    case 'CBD_testing_2':
                        $this->assertGreaterThan(0, $collection->count([]), 'CBD_testing_2 collection did not have at least 1 document in data source ' . $source);
                        break;
                }

            }
        }
    }

    public function testTransactionLogIsWrittenToCorrectDBAndCollection()
    {
        $storeName = 'tripod_php_testing';
        $newConfig = Tripod\Config::getConfig();
        $newConfig['transaction_log']['database'] = 'tripod_php_testing_transaction_log';
        $newConfig['transaction_log']['collection'] = 'transaction_log';

        Tripod\Config::setConfig($newConfig);

        $config = Tripod\Config::getInstance();

        // Clear out any old data
        $tlogDB = $config->getTransactionLogDatabase();
        $tlogDB->drop();

        // Make sure the dbs do not exist
        $transactionConnInfo = $newConfig['data_sources'][$newConfig['transaction_log']['data_source']];
        $options = isset($transactionConnInfo['replicaSet']) && !empty($transactionConnInfo['replicaSet']) ? ['replicaSet' => $transactionConnInfo['replicaSet']] : [];
        $transactionMongo = new Client($transactionConnInfo['connection'], $options);
        $transactionDbInfo = $transactionMongo->listDatabases();

        foreach ($transactionDbInfo as $db) {
            $this->assertNotEquals($db->getName(), $newConfig['transaction_log']['database']);
        }

        $tqueuesConnInfo = $newConfig['data_sources'][$newConfig['transaction_log']['data_source']];
        $options = isset($tqueuesConnInfo['replicaSet']) && !empty($tqueuesConnInfo['replicaSet']) ? ['replicaSet' => $tqueuesConnInfo['replicaSet']] : [];
        $queuesMongo = new Client($tqueuesConnInfo['connection'], $options);
        $queuesDbInfo = $queuesMongo->listDatabases();
        foreach ($queuesDbInfo as $db) {
            $this->assertNotEquals($db->getName(), $newConfig['transaction_log']['database']);
        }

        // Start adding some data
        $this->tripod = new Tripod\Mongo\Driver('CBD_testing', $storeName, [OP_ASYNC => [OP_VIEWS => true, OP_TABLES => false, OP_SEARCH => false]]);
        $this->loadResourceDataViaTripod();

        $graph = new Tripod\Mongo\MongoGraph();
        $subject = 'http://example.com/' . uniqid();
        $labeller = new Tripod\Mongo\Labeller();
        $graph->add_resource_triple($subject, RDF_TYPE, $labeller->qname_to_uri('foaf:Person'));
        $graph->add_literal_triple($subject, FOAF_NAME, 'Anne Example');
        $this->tripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $newGraph = $this->tripod->describeResource($subject);
        $newGraph->add_literal_triple($subject, $labeller->qname_to_uri('foaf:email'), 'anne@example.com');
        $this->tripod->saveChanges($graph, $newGraph);

        // Make sure the dbs do now exist
        $transactionDbInfo = $transactionMongo->listDatabases();
        $transactionDbExists = false;
        foreach ($transactionDbInfo as $db) {
            if ($db->getName() === $newConfig['transaction_log']['database']) {
                $transactionDbExists = true;
            }
        }
        $this->assertTrue($transactionDbExists);

        // Make sure the data in the dbs look right
        $transactionColletion = $transactionMongo->selectCollection($newConfig['transaction_log']['database'], $newConfig['transaction_log']['collection']);
        $transactionCount = $transactionColletion->count();
        $transactionExampleDocument = $transactionColletion->findOne();
        $this->assertEquals(26, $transactionCount);
        $this->assertStringContainsString('transaction_', $transactionExampleDocument['_id']);
    }

    public function testComputedFieldSpecValidationInvalidFunction()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['shazzbot' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$computedFieldFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed field spec does not contain valid function');
        Tripod\Config::getInstance();
    }

    public function testComputedFieldSpecValidationMultipleFunctions()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => [], 'replace' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$computedFieldFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed field spec contains more than one function');
        Tripod\Config::getInstance();
    }

    public function testComputedFieldSpecValidationMustBeAtBaseLevel()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['joins']['dct:isVersionOf']['computed_fields'] = [$computedFieldFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Table spec can only contain 'computed_fields' at the base level");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationEmptyConditional()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed conditional spec does not contain an 'if' value");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationMissingThenElse()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => []]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed conditional spec must contain a then or else value');
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationEmptyIf()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => [], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfNotArray()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => 'foo', 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value must be an array");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasTwoValues()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['foo', '*'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasMoreThanThreeValues()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', 'b', 'c', 'd'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidConditionalOperator()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '*', 'c'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Invalid conditional operator '*' in conditional spec");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidVariableAsLeftOperand()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['$wibble', '>=', 'c'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidVariableAsRightOperand()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', 'contains', '$wibble'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationThenHasInvalidVariable()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '<', 'b'], 'then' => '$wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testConditionalSpecValidationElseHasInvalidVariable()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '<', 'b'], 'then' => true, 'else' => '$wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationEmptyFunction()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'search' value");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationMissingReplace()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'replace' value");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationMissingSubject()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'subject' value");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSearch()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => '$x', 'replace' => 'y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSearchArray()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => ['a', 'b', '$x'], 'replace' => 'y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInReplace()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => '$y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$y' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInReplaceArray()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => ['a', '$y', 'c'], 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$y' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSubject()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y', 'subject' => '$z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$z' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSubjectArray()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y', 'subject' => ['$z', 'b', 'c']]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$z' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationEmptyFunction()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationOneValue()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationTwoValues()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '+']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationFourValues()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '+', 3, 4]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidArithmeticOperator()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, 'x', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Invalid arithmetic operator 'x' in computed arithmetic spec");
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidVariableLeftOperand()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => ['$x', '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidVariableRightOperand()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '*', '$x']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidNestedVariable()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [['$x', '-', 100], '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Tripod\Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidNestedOperator()
    {
        $newConfig = Tripod\Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [[101, '#', 100], '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Tripod\Config::setConfig($newConfig);
        $this->expectException(Tripod\Exceptions\ConfigException::class);
        $this->expectExceptionMessage("Invalid arithmetic operator '#' in computed arithmetic spec");
        Tripod\Config::getInstance();
    }

    public function testGetResqueServer()
    {
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);

        if (!getenv(MONGO_TRIPOD_RESQUE_SERVER)) {
            putenv(MONGO_TRIPOD_RESQUE_SERVER . '=redis');
        }
        $this->assertEquals(getenv(MONGO_TRIPOD_RESQUE_SERVER), Tripod\Mongo\Config::getResqueServer());
    }

    // MongoClient creation tests
    public function testMongoConnectionNoExceptions()
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig(json_decode(file_get_contents(dirname(__FILE__) . '/data/config.json'), true));
        $mockConfig->expects($this->exactly(1))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])
            ->will($this->returnCallback(
                function () {
                    return new Client();
                }
            ));
        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_NEAREST);
    }

    public function testMongoConnectionExceptionThrown()
    {
        $this->expectException(ConnectionTimeoutException::class);
        $this->expectExceptionMessage('Exception thrown when connecting to Mongo');
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig(json_decode(file_get_contents(dirname(__FILE__) . '/data/config.json'), true));
        $mockConfig->expects($this->exactly(30))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])
            ->will($this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')));

        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::RP_SECONDARY_PREFERRED);
    }

    public function testMongoConnectionNoExceptionThrownWhenConnectionThrowsSomeExceptions()
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig(json_decode(file_get_contents(dirname(__FILE__) . '/data/config.json'), true));
        $mockConfig->expects($this->exactly(5))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])
            ->will($this->onConsecutiveCalls(
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->returnCallback(
                    function () {
                        return new Client();
                    }
                )
            ));

        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::RP_NEAREST);
    }
}
