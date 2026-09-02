<?php

declare(strict_types=1);

use MongoDB\Client;
use MongoDB\Driver\Exception\ConnectionTimeoutException;
use MongoDB\Driver\ReadPreference;
use Tripod\Config;
use Tripod\Exceptions\ConfigException;
use Tripod\ExtendedGraph;
use Tripod\Mongo\Driver;
use Tripod\Mongo\IConfigInstance;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\MongoSearchProvider;

class MongoTripodConfigUnitTest extends MongoTripodTestBase
{
    private IConfigInstance $tripodConfig;

    protected function setUp(): void
    {
        parent::setup();
        $this->tripodConfig = Config::getInstance();
    }

    public function testGetInstanceThrowsExceptionIfSetInstanceNotCalledFirst(): void
    {
        // to test that the instance throws an exception if it is called before calling setConfig
        // i first have to destroy the instance that is created in the setUp() method of our test suite.

        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Call Config::setConfig() first');
        unset($this->tripodConfig);

        Config::destroy();
        Config::getInstance();
    }

    public function testNamespaces(): void
    {
        $ns = $this->tripodConfig->getNamespaces();
        $this->assertCount(16, $ns, 'Incorrect number of namespaces');

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
        $this->assertSame($expectedNs, $ns, 'Incorrect namespaces');
    }

    public function testTConfig(): void
    {
        $config = Config::getInstance();
        Config::getConfig();
        $tConfig = $config->getTransactionLogConfig();
        $this->assertEquals('tripod_php_testing', $tConfig['database']);
        $this->assertEquals('transaction_log', $tConfig['collection']);
    }

    public function testCardinality(): void
    {
        $cardinality = $this->tripodConfig->getCardinality('tripod_php_testing', 'CBD_testing', 'dct:created');
        $this->assertEquals(1, $cardinality, 'Expected cardinality of 1 for dct:created');

        $cardinality = $this->tripodConfig->getCardinality('tripod_php_testing', 'CBD_testing', 'random:property');
        $this->assertEquals(-1, $cardinality, 'Expected cardinality of 1 for random:property');
    }

    public function testCompoundIndexAllArraysThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
                                'dct:subject.value' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ];

        Config::setConfig($config);
        Config::getInstance();
    }

    public function testSearchConfig(): void
    {
        $config = Config::getInstance();
        $this->assertSame(MongoSearchProvider::class, $config->getSearchProviderClassName('tripod_php_testing'));

        $this->assertCount(3, $config->getSearchDocumentSpecifications('tripod_php_testing'));
        $this->assertCount(2, $config->getCollectionsForSearch('tripod_php_testing'));
    }

    /**
     * @testWith ["Tripod\\Mongo\\MongoSearchProvider"]
     *           ["\\Tripod\\Mongo\\MongoSearchProvider"]
     */
    public function testSearchConfigTrimsSearchProviderLeadingBackslash(string $providerClassName): void
    {
        Config::setConfig([
            'defaultContext' => 'http://talisaspire.com/',
            'data_sources' => [
                'mongo1' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://mongodb',
                ],
            ],
            'stores' => [
                'tripod_php_testing' => [
                    'data_source' => 'mongo1',
                    'pods' => [
                        'CBD_testing' => [],
                    ],
                    'search_config' => [
                        'search_provider' => $providerClassName,
                        'search_specifications' => [
                            [
                                '_id' => 'i_search_list',
                                'from' => 'CBD_testing',
                                'filter' => [],
                                'fields' => [],
                            ],
                        ],
                    ],
                ],
            ],
            'transaction_log' => [
                'database' => 'transactions',
                'collection' => 'transaction_log',
                'data_source' => 'mongo1',
            ],
        ]);
        $mtc = Config::getInstance();
        $this->assertEquals(MongoSearchProvider::class, $mtc->getSearchProviderClassName('tripod_php_testing'));
    }

    public function testCardinalityRuleWithNoNamespace(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testGetSearchDocumentSpecificationsByType(): void
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
        $actualSpec = Config::getInstance()->getSearchDocumentSpecifications('tripod_php_testing', 'resourcelist:List');
        $this->assertEquals($expectedSpec, $actualSpec);
    }

    public function testGetSearchDocumentSpecificationsById(): void
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
        $actualSpec = Config::getInstance()->getSearchDocumentSpecification('tripod_php_testing', 'i_search_list');
        $this->assertEquals($expectedSpec, $actualSpec);
    }

    public function testGetSearchDocumentSpecificationsWhereNoneExists(): void
    {
        $expectedSpec = [];
        $actualSpec = Config::getInstance()->getSearchDocumentSpecifications('something:doesntexist');
        $this->assertSame($expectedSpec, $actualSpec);
    }

    public function testViewSpecCountWithoutTTLThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
                    'CBD_testing' => [],
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testViewSpecCountNestedInJoinWithoutTTLThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
                    'CBD_testing' => [],
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecNestedCountWithoutPropertyThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecNested2ndLevelCountWithoutFieldNameThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecFieldWithoutFieldName(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecFieldWithoutPredicates(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecCountWithoutProperty(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecCountWithoutFieldName(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testTableSpecCountWithoutPropertyAsAString(): void
    {
        $this->expectException(ConfigException::class);
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
        Config::setConfig($config);
        Config::getInstance();
    }

    public function testConfigWithoutDefaultNamespaceThrowsException(): void
    {
        $this->expectException(ConfigException::class);
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
                    'CBD_testing' => [],
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
        Config::setConfig($config);
        Config::getInstance();
    }

    /**
     * the indexesGroupedByCollection method should not only return each of the indexes that are defined explicitly in the config.json,
     * but also include indexes that are inserted by Config object because they are needed by tripod.
     */
    public function testGetIndexesGroupedByCollection(): void
    {
        $indexSpecs = Config::getInstance()->getIndexesGroupedByCollection('tripod_php_testing');

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

    public function testGetReplicaSetName(): void
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
                    'CBD_testing' => [],
                ],
            ],
            'testing_2' => [
                'data_source' => 'mongo1',
                'pods' => [
                    'CBD_testing' => [],
                ],
            ],
        ];
        Config::setConfig($config);

        /** @var Tripod\Mongo\Config */
        $mtc = Config::getInstance();
        $this->assertEquals('myreplicaset', $mtc->getReplicaSetName((string) $mtc->getDefaultDataSourceForStore('tripod_php_testing')));

        $this->assertNull($mtc->getReplicaSetName((string) $mtc->getDefaultDataSourceForStore('testing_2')));
    }

    public function testGetReplicaSetNameNonExistingDatasource(): void
    {
        Config::setConfig([
            'defaultContext' => 'http://talisaspire.com/',
            'data_sources' => [
                'tlog' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://abc:zyx@localhost:27018',
                ],
            ],
            'transaction_log' => [
                'database' => 'transactions',
                'collection' => 'transaction_log',
                'data_source' => 'tlog',
            ],
            'stores' => [],
        ]);

        /** @var Tripod\Mongo\Config */
        $mtc = Config::getInstance();

        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Data source 'non_existing_data_source' not in configuration");
        $mtc->getReplicaSetName('non_existing_data_source');
    }

    public function testGetReplicaSetNameFromConnectionString(): void
    {
        Config::setConfig([
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
        $mtc = Config::getInstance();
        $this->assertEquals('myReplicaSet', $mtc->getReplicaSetName('rs1'));
        $this->assertEquals(null, $mtc->getReplicaSetName('rs2'));
    }

    public function testGetViewSpecification(): void
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
                '_id' => [
                    'from' => 'CBD_test_related_content',
                    'include' => ['dct:title'],
                ],
            ],
        ];

        $vspec = Config::getInstance()->getViewSpecification('tripod_php_testing', 'v_resource_full');
        $this->assertEquals($expectedVspec, $vspec);

        $vspec = Config::getInstance()->getViewSpecification('tripod_php_testing', 'doesnt_exist');
        $this->assertNull($vspec);
    }

    public function testGetTableSpecification(): void
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

        $tspec = Config::getInstance()->getTableSpecification('tripod_php_testing', 't_resource');
        $this->assertEquals($expectedTspec, $tspec);

        $tspec = Config::getInstance()->getTableSpecification('tripod_php_testing', 'doesnt_exist');
        $this->assertNull($tspec);
    }

    public function testSearchConfigNotPresent(): void
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

        Config::setConfig($config);
        $mtc = Config::getInstance();
        $this->assertNull($mtc->getSearchProviderClassName('tripod_php_testing'));
        $this->assertSame([], $mtc->getSearchDocumentSpecifications('tripod_php_testing'));
    }

    public function testGetAllTypesInSpecifications(): void
    {
        $types = $this->tripodConfig->getAllTypesInSpecifications('tripod_php_testing');
        $this->assertCount(
            12,
            $types,
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
            $this->assertContains($expected, $types, 'List of types should have contained ' . $expected);
        }
    }

    public function testGetPredicatesForTableSpec(): void
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 't_users');
        $this->assertCount(6, $predicates, 'There should be 6 predicates defined in t_users in config.json');
        $expectedValues = [
            'rdf:type',
            'foaf:firstName',
            'foaf:surname',
            'temp:last_login',
            'temp:last_login_invalid',
            'temp:last_login_DOES_NOT_EXIST',
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, 'List of predicates should have contained ' . $expected);
        }
    }

    public function testGetPredicatesForSearchDocSpec(): void
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 'i_search_list');
        $this->assertCount(6, $predicates, 'There should be 6 predicates defined in i_search_list in config.json');

        $expectedValues = [
            'rdf:type',
            'spec:name',
            'resourcelist:description',
            'resourcelist:usedBy', // defined in the join
            'aiiso:name',
            'aiiso:code',
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, 'List of predicates should have contained ' . $expected);
        }
    }

    public function testGetPredicatesForSpecFilter(): void
    {
        $predicates = $this->tripodConfig->getDefinedPredicatesInSpec('tripod_php_testing', 'i_search_filter_parse');

        $this->assertCount(6, $predicates, 'There should be 6 predicates defined in i_search_filter_parse in config.json');

        $expectedValues = [
            'rdf:type',
            'spec:name',
            'dct:title',
            'dct:created', // defined only in the filter
            'temp:numberOfThings', // defined only in the filter
            'temp:amountOfTimeSpent', // defined only in the filter
        ];

        foreach ($expectedValues as $expected) {
            $this->assertContains($expected, $predicates, 'List of predicates should have contained ' . $expected);
        }
    }

    public function testCollectionReadPreferencesAreAppliedToDatabase(): void
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getDatabase'])
            ->getMock();
        $mockConfig->loadConfig($this->decodeJsonFile(__DIR__ . '/data/config.json'));
        $mockConfig->expects($this->exactly(2))
            ->method('getDatabase')
            ->withConsecutive(
                ['tripod_php_testing', 'rs1', ReadPreference::SECONDARY_PREFERRED],
                ['tripod_php_testing', 'rs1', ReadPreference::NEAREST]
            )
            ->willReturnCallback(function () {
                $mongo = new Client();

                return $mongo->selectDatabase('tripod_php_testing');
            });

        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::NEAREST);
    }

    public function testDataLoadedInConfiguredDataSource(): void
    {
        $storeName = 'tripod_php_testing';

        $dataSourcesForStore = [];

        /** @var Tripod\Mongo\Config */
        $config = Config::getInstance();
        $pods = $config->getPods($storeName);

        foreach ($pods as $pod) {
            if (!in_array($config->getDataSourceForPod($storeName, $pod), $dataSourcesForStore)) {
                $dataSourcesForStore[] = $config->getDataSourceForPod($storeName, $pod);
            }
        }

        foreach ($config->getViewSpecifications($storeName) as $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach ($config->getTableSpecifications($storeName) as $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        foreach ($config->getSearchDocumentSpecifications($storeName) as $spec) {
            if (!in_array($spec['to_data_source'], $dataSourcesForStore)) {
                $dataSourcesForStore[] = $spec['to_data_source'];
            }
        }

        if (count($dataSourcesForStore) < 2) {
            $this->markTestSkipped('Less than two datasources configured for store, nothing to test');
        }

        $diff = false;

        $cfg = Config::getConfig();
        $defaultDataSource = $cfg['data_sources'][$config->getDefaultDataSourceForStore($storeName)];

        foreach ($dataSourcesForStore as $source) {
            if ($cfg['data_sources'][$source] != $defaultDataSource) {
                $diff = true;

                break;
            }

            $config->getDatabase($storeName, $source)->drop();
        }

        if ($diff === false) {
            $this->markTestSkipped('All datasources configured for store use same configuration, nothing to test');
        }

        $this->tripod = new Driver('CBD_testing', $storeName, [OP_ASYNC => [OP_VIEWS => true, OP_TABLES => false, OP_SEARCH => false]]);
        $this->loadResourceDataViaTripod();

        $tripod2 = new Driver('CBD_testing_2', $storeName, [OP_ASYNC => [OP_VIEWS => true, OP_TABLES => false, OP_SEARCH => false]]);

        $graph = new MongoGraph();
        $subject = 'http://example.com/' . uniqid();
        $labeller = new Labeller();
        $graph->add_resource_triple($subject, RDF_TYPE, $labeller->qname_to_uri('foaf:Person'));
        $graph->add_literal_triple($subject, FOAF_NAME, 'Anne Example');

        $this->tripod->saveChanges(new ExtendedGraph(), $graph);
        $tripod2->saveChanges(new ExtendedGraph(), $graph);

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

        $collectionsForDataSource = [
            'rs1' => [
                VIEWS_COLLECTION,
                SEARCH_INDEX_COLLECTION,
                TABLE_ROWS_COLLECTION,
                'CBD_testing',
                AUDIT_MANUAL_ROLLBACKS_COLLECTION,
                LOCKS_COLLECTION,
            ],
            'rs2' => [
                VIEWS_COLLECTION,
                SEARCH_INDEX_COLLECTION,
                TABLE_ROWS_COLLECTION,
                'CBD_testing_2',
                'transaction_log',
            ],
        ];

        $specs = [
            'views' => Config::getInstance()->getViewSpecifications($storeName),
            'search' => Config::getInstance()->getSearchDocumentSpecifications($storeName),
            'table_rows' => Config::getInstance()->getTableSpecifications($storeName),
        ];
        $specsForDataSource = [];

        foreach (['views', 'search', 'table_rows'] as $type) {
            foreach ($specs[$type] as $spec) {
                if (!isset($specsForDataSource[$spec['to_data_source']])) {
                    $specsForDataSource[$spec['to_data_source']] = ['views' => [], 'search' => [], 'table_rows' => []];
                }

                $specsForDataSource[$spec['to_data_source']][$type][] = $spec['_id'];
            }
        }

        $foundCollections = [];

        foreach ($dataSourcesForStore as $source) {
            $db = $config->getDatabase($storeName, $source);
            foreach ($db->listCollections() as $collectionInfo) {
                $collectionName = $collectionInfo->getName();
                if (strpos($collectionName, 'system.') === 0) {
                    continue;
                }

                $collection = $db->selectCollection($collectionName);
                $foundCollections[] = $collectionName;
                $this->assertContains($collectionName, $collectionsForDataSource[$source], 'Source ' . $source . ' does not include ' . $collectionName);

                switch ($collectionName) {
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

    public function testTransactionLogIsWrittenToCorrectDBAndCollection(): void
    {
        $storeName = 'tripod_php_testing';
        $newConfig = Config::getConfig();
        $newConfig['transaction_log']['database'] = 'tripod_php_testing_transaction_log';
        $newConfig['transaction_log']['collection'] = 'transaction_log';

        Config::setConfig($newConfig);

        $config = Config::getInstance();

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
        $this->tripod = new Driver('CBD_testing', $storeName, [OP_ASYNC => [OP_VIEWS => true, OP_TABLES => false, OP_SEARCH => false]]);
        $this->loadResourceDataViaTripod();

        $graph = new MongoGraph();
        $subject = 'http://example.com/' . uniqid();
        $labeller = new Labeller();
        $graph->add_resource_triple($subject, RDF_TYPE, $labeller->qname_to_uri('foaf:Person'));
        $graph->add_literal_triple($subject, FOAF_NAME, 'Anne Example');

        $this->tripod->saveChanges(new ExtendedGraph(), $graph);

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
        $this->assertNotNull($transactionExampleDocument);
        $this->assertEquals(26, $transactionCount);
        $this->assertIsString($transactionExampleDocument['_id']);
        $this->assertStringContainsString('transaction_', $transactionExampleDocument['_id']);
    }

    public function testComputedFieldSpecValidationInvalidFunction(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['shazzbot' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$computedFieldFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed field spec does not contain valid function');
        Config::getInstance();
    }

    public function testComputedFieldSpecValidationMultipleFunctions(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => [], 'replace' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$computedFieldFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed field spec contains more than one function');
        Config::getInstance();
    }

    public function testComputedFieldSpecValidationMustBeAtBaseLevel(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $computedFieldFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['joins']['dct:isVersionOf']['computed_fields'] = [$computedFieldFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Table spec can only contain 'computed_fields' at the base level");
        Config::getInstance();
    }

    public function testConditionalSpecValidationEmptyConditional(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed conditional spec does not contain an 'if' value");
        Config::getInstance();
    }

    public function testConditionalSpecValidationMissingThenElse(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => []]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed conditional spec must contain a then or else value');
        Config::getInstance();
    }

    public function testConditionalSpecValidationEmptyIf(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => [], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfNotArray(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => 'foo', 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value must be an array");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasTwoValues(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['foo', '*'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasMoreThanThreeValues(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', 'b', 'c', 'd'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed conditional field spec 'if' value array must have 1 or 3 values");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidConditionalOperator(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '*', 'c'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Invalid conditional operator '*' in conditional spec");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidVariableAsLeftOperand(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['$wibble', '>=', 'c'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Config::getInstance();
    }

    public function testConditionalSpecValidationIfHasInvalidVariableAsRightOperand(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', 'contains', '$wibble'], 'then' => 'wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Config::getInstance();
    }

    public function testConditionalSpecValidationThenHasInvalidVariable(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '<', 'b'], 'then' => '$wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Config::getInstance();
    }

    public function testConditionalSpecValidationElseHasInvalidVariable(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $conditionalFunction = ['fieldName' => 'fooBar', 'value' => ['conditional' => ['if' => ['a', '<', 'b'], 'then' => true, 'else' => '$wibble']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$conditionalFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$wibble' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationEmptyFunction(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'search' value");
        Config::getInstance();
    }

    public function testReplaceSpecValidationMissingReplace(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'replace' value");
        Config::getInstance();
    }

    public function testReplaceSpecValidationMissingSubject(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed replace spec does not contain 'subject' value");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSearch(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => '$x', 'replace' => 'y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSearchArray(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => ['a', 'b', '$x'], 'replace' => 'y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInReplace(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => '$y', 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$y' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInReplaceArray(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => ['a', '$y', 'c'], 'subject' => 'z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$y' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSubject(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y', 'subject' => '$z']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$z' is not defined in table spec");
        Config::getInstance();
    }

    public function testReplaceSpecValidationInvalidVariableInSubjectArray(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $replaceFunction = ['fieldName' => 'fooBar', 'value' => ['replace' => ['search' => 'x', 'replace' => 'y', 'subject' => ['$z', 'b', 'c']]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$replaceFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$z' is not defined in table spec");
        Config::getInstance();
    }

    public function testArithmeticSpecValidationEmptyFunction(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => []]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Config::getInstance();
    }

    public function testArithmeticSpecValidationOneValue(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Config::getInstance();
    }

    public function testArithmeticSpecValidationTwoValues(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '+']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Config::getInstance();
    }

    public function testArithmeticSpecValidationFourValues(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '+', 3, 4]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('Computed arithmetic spec must contain 3 values');
        Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidArithmeticOperator(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, 'x', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Invalid arithmetic operator 'x' in computed arithmetic spec");
        Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidVariableLeftOperand(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => ['$x', '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidVariableRightOperand(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [1, '*', '$x']]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidNestedVariable(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [['$x', '-', 100], '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Computed spec variable '\$x' is not defined in table spec");
        Config::getInstance();
    }

    public function testArithmeticSpecValidationInvalidNestedOperator(): void
    {
        $newConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
        $arithmeticFunction = ['fieldName' => 'fooBar', 'value' => ['arithmetic' => [[101, '#', 100], '*', 3]]];
        $newConfig['stores']['tripod_php_testing']['table_specifications'][0]['computed_fields'] = [$arithmeticFunction];
        Config::setConfig($newConfig);
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Invalid arithmetic operator '#' in computed arithmetic spec");
        Config::getInstance();
    }

    public function testGetResqueServer(): void
    {
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);

        if (!getenv(MONGO_TRIPOD_RESQUE_SERVER)) {
            putenv(MONGO_TRIPOD_RESQUE_SERVER . '=redis');
        }

        $this->assertEquals(getenv(MONGO_TRIPOD_RESQUE_SERVER), Tripod\Mongo\Config::getResqueServer());
    }

    // MongoClient creation tests
    public function testMongoConnectionNoExceptions(): void
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig($this->decodeJsonFile(__DIR__ . '/data/config.json'));
        $mockConfig->expects($this->exactly(1))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])
            ->willReturnCallback(fn (): Client => new Client());
        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::NEAREST);
    }

    public function testMongoConnectionExceptionThrown(): void
    {
        $this->expectException(ConnectionTimeoutException::class);
        $this->expectExceptionMessage('Exception thrown when connecting to Mongo');
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig($this->decodeJsonFile(__DIR__ . '/data/config.json'));
        $mockConfig->expects($this->exactly(30))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])
            ->willThrowException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo'));

        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::SECONDARY_PREFERRED);
    }

    public function testMongoConnectionNoExceptionThrownWhenConnectionThrowsSomeExceptions(): void
    {
        $mockConfig = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getMongoClient'])
            ->getMock();
        $mockConfig->loadConfig($this->decodeJsonFile(__DIR__ . '/data/config.json'));
        $mockConfig->expects($this->exactly(5))
            ->method('getMongoClient')
            ->with('mongodb://mongodb:27017/', ['connectTimeoutMS' => 20000])->willReturnOnConsecutiveCalls(
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->throwException(new ConnectionTimeoutException('Exception thrown when connecting to Mongo')),
                $this->returnCallback(
                    fn (): Client => new Client()
                )
            );

        $mockConfig->getDatabase('tripod_php_testing', 'rs1', ReadPreference::SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::SECONDARY_PREFERRED);
        $mockConfig->getCollectionForCBD('tripod_php_testing', 'CBD_testing', ReadPreference::NEAREST);
    }
}
