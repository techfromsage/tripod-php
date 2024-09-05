<?php

use PHPUnit\Framework\MockObject\MockObject;

class IndexUtilsTest extends MongoTripodTestBase
{
    public function testCBDCollectionIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testCBDCollectionIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, false);
        $this->getCollectionForCBDShouldBeCalled_n_Times(4, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testCBDCollectionIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($collection, true);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testViewIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, false);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testViewIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForViewIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($collection, true);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testTableIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, false);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testTableIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForTableIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($collection, true);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForSearchDocumentShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreated()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', true);
    }

    public function testSearchDocIndexesAreCreatedInForeground()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldNeverBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, false);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(false, 'tripod_php_testing', false);
    }

    public function testSearchDocIndexesAreReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForSearchDocIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->threeInternalTripodSearchDocIndexesShouldBeCreated($collection, true);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForCBDShouldNeverBeCalled($config);
        $this->getCollectionForViewShouldNeverBeCalled($config);
        $this->getCollectionForTableShouldNeverBeCalled($config);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    public function testIndexesAreDroppedOnlyOncePerCollectionWhenReindexed()
    {
        $config = $this->createMockConfig();
        $collection = $this->createMockCollection();
        $indexUtils = $this->createMockIndexUtils($config);

        $this->setConfigForCBDViewTableAndSearchDocIndexes($config);
        $this->dropIndexesShouldBeCalled($collection);
        $this->getCollectionForCBDShouldBeCalled_n_Times(5, $config, $collection);
        $this->getCollectionForViewShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForTableShouldBeCalled_n_Times(1, $config, $collection);
        $this->getCollectionForSearchDocShouldBeCalled_n_Times(1, $config, $collection);

        $indexUtils->ensureIndexes(true, 'tripod_php_testing', true);
    }

    // HELPER METHODS

    /**
     * creates a mock IndexUtils object which will use the specified config
     *
     * @param MockObject&\TripodTestConfig $mockConfig mock config object
     * @return MockObject&\Tripod\Mongo\IndexUtils mocked IndexUtil object
     */
    protected function createMockIndexUtils($mockConfig)
    {
        $mockIndexUtils = $this->getMockBuilder(Tripod\Mongo\IndexUtils::class)
            ->onlyMethods(['getConfig'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('getConfig')
            ->will($this->returnValue($mockConfig));

        return $mockIndexUtils;
    }

    /**
     * creates a mock mongo collection object
     *
     * @return MockObject&Collection mock Collection object
     */
    protected function createMockCollection()
    {
        return $this->getMockBuilder(MongoDB\Collection::class)
            ->onlyMethods(['createIndex', 'dropIndexes'])
            ->setConstructorArgs([
                new MongoDB\Driver\Manager('mongodb://fake:27017'),
                'tripod_php_testing',
                'CBD_testing',
            ])
            ->getMock();
    }

    /**
     * creates a mock config object
     *
     * @return MockObject&\TripodTestConfig mock Config object
     */
    protected function createMockConfig()
    {
        return $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods([
                'getCollectionForCBD',
                'getCollectionForView',
                'getCollectionForTable',
                'getCollectionForSearchDocument',
            ])
            ->getMock();
    }

    /**
     * @param int $callCount number of times Config->getCollectionForCBD should be called
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function getCollectionForCBDShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForCBD')
            ->with('tripod_php_testing', 'CBD_testing')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int $callCount number of times Config->getCollectionForView should be called
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function getCollectionForViewShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForView')
            ->with('tripod_php_testing', 'v_testview')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int $callCount number of times Config->getCollectionForTable should be called
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function getCollectionForTableShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForTable')
            ->with('tripod_php_testing', 't_testtable')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param int $callCount number of times Config->getCollectionForSearchDocument should be called
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function getCollectionForSearchDocShouldBeCalled_n_Times($callCount, $mockConfig, $mockCollection)
    {
        $mockConfig->expects($this->exactly($callCount))
            ->method('getCollectionForSearchDocument')
            ->with('tripod_php_testing', 'i_search_something')
            ->will($this->returnValue($mockCollection));
    }

    /**
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function dropIndexesShouldNeverBeCalled($mockCollection)
    {
        $mockCollection->expects($this->never())
            ->method('dropIndexes');
    }

    /**
     * @param MockObject&Collection $mockCollection mock Collection object
     * @return void
     */
    protected function dropIndexesShouldBeCalled($mockCollection)
    {
        $mockCollection->expects($this->once())
            ->method('dropIndexes');
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function getCollectionForViewShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForView');
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function getCollectionForTableShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForTable');
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function getCollectionForSearchDocumentShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForSearchDocument');
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function getCollectionForCBDShouldNeverBeCalled($mockConfig)
    {
        $mockConfig->expects($this->never())
            ->method('getCollectionForCBD');
    }

    /**
     * Expectations one custom and three internal tripod indexes should be
     * created
     *
     * createIndex is called 4 times, each time with a different set of params
     * a) one custom index is created based on the collection specification
     * b) three internal indexes are always created
     *
     * @param MockObject&Collection $mockCollection mock Collection object
     * @param bool $background create indexes in the background
     * @return void
     */
    protected function oneCustomAndThreeInternalTripodCBDIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the collection specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                [['rdf:type.u' => 1], ['name' => 'rdf_type', 'background' => $background]],
                [[_ID_KEY => 1, _LOCKED_FOR_TRANS => 1], ['name' => '_lockedForTransIdx', 'background' => $background]],
                [[_ID_KEY => 1, _UPDATED_TS => 1], ['name' => '_updatedTsIdx', 'background' => $background]],
                [[_ID_KEY => 1, _CREATED_TS => 1], ['name' => '_createdTsIdx', 'background' => $background]]
            );
    }

    /**
     * @param MockObject&Collection $mockCollection mock Collection object
     * @param bool $background create indexes in the background
     * @return void
     */
    protected function oneCustomAndThreeInternalTripodViewIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(5))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1, _ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [['value.' . _IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]],
                [['rdf:type.u' => 1, '_cts' => 1], ['background' => $background]]
            );
    }

    /**
     * @param MockObject&Collection $mockCollection mock Collection object
     * @param bool $background create indexes in the background
     * @return void
     */
    protected function oneCustomAndThreeInternalTripodTableIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 4 times, each time with a different set of
        // params that we know.
        // a) one custom index is created based on the view specification
        // b) three internal indexes are always created
        $mockCollection->expects($this->exactly(5))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1, _ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [['value.' . _IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]],
                [['rdf:type.u' => 1], ['background' => $background]]
            );
    }

    /**
     * @param MockObject&Collection $mockCollection mock Collection object
     * @param bool $background create indexes in the background
     * @return void
     */
    protected function threeInternalTripodSearchDocIndexesShouldBeCreated($mockCollection, $background = true)
    {
        // create index is called 3 times, each time with a different set of
        // params that we know.
        // for search docs only internal indexes are created
        $mockCollection->expects($this->exactly(4))
            ->method('createIndex')
            ->withConsecutive(
                [[_ID_KEY . '.' . _ID_RESOURCE => 1, _ID_KEY . '.' . _ID_CONTEXT => 1], ['background' => $background]],
                [[_ID_KEY . '.' . _ID_TYPE => 1], ['background' => $background]],
                [[_IMPACT_INDEX => 1], ['background' => $background]],
                [['_cts' => 1], ['background' => $background]]
            );

    }

    /**
     * Returns tripod config to use on with the IndexUtils object
     * This is a minimal config used to assert what should happen when ensuring
     * indexes for a CBD collection
     *
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function setConfigForCBDIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => [
                    'CBD_testing' => [
                        'indexes' => [
                            'rdf_type' => [
                                'rdf:type.u' => 1,
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function setConfigForViewIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'view_specifications' => [
                    [
                        '_id' => 'v_testview',
                        'ensureIndexes' => [
                            ['rdf:type.u' => 1, '_cts' => 1],
                        ],
                        'from' => 'CBD_testing',
                        'type' => 'temp:TestType',
                        'joins' => [
                            'dct:partOf' => [],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function setConfigForTableIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'table_specifications' => [
                    [
                        '_id' => 't_testtable',
                        'ensureIndexes' => [
                            ['rdf:type.u' => 1],
                        ],
                        'from' => 'CBD_testing',
                        'type' => 'temp:TestType',
                        'fields' => [
                            [
                                'fieldName' => 'fieldA',
                                'predicates' => ['spec:note'],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     * @return void
     */
    protected function setConfigForSearchDocIndexes($mockConfig)
    {
        // minimal config to verify that
        $config = [];
        $config['data_sources'] = [
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018/admin',
                'replicaSet' => 'tlogrepset',
            ],
            'mongo' => ['type' => 'mongo', 'connection' => 'mongodb://localhost'],
        ];
        $config['defaultContext'] = 'http://talisaspire.com/';
        $config['stores'] = [
            'tripod_php_testing' => [
                'type' => 'mongo',
                'data_source' => 'mongo',
                'pods' => ['CBD_testing' => []],
                'search_config' => [
                    'search_provider' => 'MongoSearchProvider',
                    'search_specifications' => [
                        [
                            '_id' => 'i_search_something',
                            'type' => 'temp:TestType',
                            'from' => 'CBD_testing',
                            'filter' => [
                                'from' => 'CBD_testing',
                                'condition' => [
                                    'spec:name' => [
                                        '$exists' => true,
                                    ],
                                ],
                            ],
                            'fields' => [
                                [
                                    'fieldName' => 'result.title',
                                    'predicates' => ['spec:note'],
                                    'limit' => 1,
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $config['transaction_log'] = [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'tlog',
        ];

        $mockConfig->loadConfig($config);
    }

    /**
     * @param MockObject&\TripodTestConfig $mockConfig mock Config object
     */
    protected function setConfigForCBDViewTableAndSearchDocIndexes($mockConfig)
    {
        $mockConfig->loadConfig([
            'defaultContext' => 'http://talisaspire.com/',
            'data_sources' => [
                'tlog' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://tloghost',
                ],
                'mongo' => [
                    'type' => 'mongo',
                    'connection' => 'mongodb://mongo',
                ],
            ],
            'stores' => [
                'tripod_php_testing' => [
                    'type' => 'mongo',
                    'data_source' => 'mongo',
                    'pods' => [
                        'CBD_testing' => [
                            'indexes' => [
                                'rdf_type' => ['rdf:type.u' => 1],
                            ],
                        ],
                    ],
                    'view_specifications' => [
                        [
                            '_id' => 'v_testview',
                            'ensureIndexes' => [
                                ['rdf:type.u' => 1, '_cts' => 1],
                            ],
                            'from' => 'CBD_testing',
                            'type' => 'temp:TestType',
                            'joins' => [
                                'dct:partOf' => [],
                            ],
                        ],
                    ],
                    'table_specifications' => [
                        [
                            '_id' => 't_testtable',
                            'ensureIndexes' => [
                                ['rdf:type.u' => 1],
                            ],
                            'from' => 'CBD_testing',
                            'type' => 'temp:TestType',
                            'fields' => [
                                [
                                    'fieldName' => 'fieldA',
                                    'predicates' => ['spec:note'],
                                ],
                            ],
                        ],
                    ],
                    'search_config' => [
                        'search_provider' => 'MongoSearchProvider',
                        'search_specifications' => [
                            [
                                '_id' => 'i_search_something',
                                'type' => 'temp:TestType',
                                'from' => 'CBD_testing',
                                'filter' => [
                                    'from' => 'CBD_testing',
                                    'condition' => [
                                        'spec:name' => ['$exists' => true],
                                    ],
                                ],
                                'fields' => [
                                    [
                                        'fieldName' => 'result.title',
                                        'predicates' => ['spec:note'],
                                        'limit' => 1,
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ],
            'transaction_log' => [
                'database' => 'transactions',
                'collection' => 'transaction_log',
                'data_source' => 'tlog',
            ],
        ]);
    }
}
