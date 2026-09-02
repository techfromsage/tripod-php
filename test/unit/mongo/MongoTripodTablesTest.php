<?php

declare(strict_types=1);

use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\DeleteResult;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Manager;
use MongoDB\UpdateResult;
use Tripod\Config;
use Tripod\Exceptions\ConfigException;
use Tripod\ExtendedGraph;
use Tripod\Mongo\Documents\Tables;
use Tripod\Mongo\Driver;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\IndexUtils;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TransactionLog;
use Tripod\Mongo\Updates;
use Tripod\Test\Mongo\Mocks\Cursor as FakeCursor;

class MongoTripodTablesTest extends MongoTripodTestBase
{
    private Tripod\Mongo\Composites\Tables $tripodTables;

    private string $defaultContext = 'http://talisaspire.com/';

    private string $defaultStoreName = 'tripod_php_testing';

    private string $defaultPodName = 'CBD_testing';

    private array $tablesConstParams;

    protected function setUp(): void
    {
        parent::setup();

        $this->tripodTransactionLog = new TransactionLog();
        $this->tripodTransactionLog->purgeAllTransactions();

        $this->tripod = new Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            ['async' => [OP_VIEWS => false, OP_TABLES => false, OP_SEARCH => false]]
        );

        $this->getTripodCollection($this->tripod)->drop();
        $this->tripod->setTransactionLog($this->tripodTransactionLog);
        $this->loadResourceDataViaTripod();
        $this->tablesConstParams = [
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            'http://talisaspire.com/',
        ];

        $this->tripodTables = new Tripod\Mongo\Composites\Tables(
            $this->tripod->getStoreName(),
            $this->getTripodCollection($this->tripod),
            null // pass null context, should default to http://talisaspire.com
        );

        // purge tables
        foreach (Config::getInstance()->getCollectionsForTables($this->tripod->getStoreName()) as $collection) {
            $collection->drop();
        }
    }

    public function testTripodSaveChangesUpdatesLiteralTripleInTable(): void
    {
        $this->tripodTables->generateTableRows('t_resource', 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2');

        $t1 = $this->tripodTables->getTableRows('t_resource', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $expectedIsbn13s = ['9780393929691', '9780393929691-2'];
        $this->assertEquals($expectedIsbn13s, $t1['results'][0]['isbn13']);

        $g1 = $this->tripod->describeResource('http://talisaspire.com/works/4d101f63c10a6');
        $g2 = $this->tripod->describeResource('http://talisaspire.com/works/4d101f63c10a6');

        $g2->add_literal_triple('http://talisaspire.com/works/4d101f63c10a6', $g2->qname_to_uri('bibo:isbn13'), '9780393929691-3');

        $this->tripod->saveChanges($g1, $g2, 'http://talisaspire.com/');

        $t2 = $this->tripodTables->getTableRows('t_resource', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $expectedIsbn13s = ['9780393929691', '9780393929691-2', '9780393929691-3'];
        $this->assertEquals($expectedIsbn13s, $t2['results'][0]['isbn13']);
    }

    public function testGenerateTableRowsWithCounts(): void
    {
        $this->tripodTables->generateTableRows('t_source_count');

        $t1 = $this->tripodTables->getTableRows('t_source_count');

        // expecting two rows
        $this->assertCount(3, $t1['results']);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertEquals(0, $result['random_predicate_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountUpdateAndRequery(): void
    {
        $this->tripodTables->generateTableRows('t_source_count');

        $t1 = $this->tripodTables->getTableRows('t_source_count');

        // expecting two rows
        $this->assertCount(3, $t1['results']);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://example.com');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_source_count');

        $result = null;
        $this->assertCount(3, $t2['results']);
        foreach ($t2['results'] as $r) {
            if ($r['_id']['r'] == $subject) {
                $result = $r;
            }
        }

        $this->assertNotNull($result, 'Cound not find table row for ' . $subject);
        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(2, $result['source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountAndRegexUpdateAndRequery(): void
    {
        $this->tripodTables->generateTableRows('t_source_count_regex');

        $t1 = $this->tripodTables->getTableRows('t_source_count_regex');

        // expecting two rows
        $this->assertCount(3, $t1['results']);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(1, $result['source_count']);
        $this->assertEquals(0, $result['regex_source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');

        $subject = $result['_id']['r'];

        $subjectGraph = $this->tripod->describeResource($subject);
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://foobarbaz.com');
        $newGraph->add_resource_triple($subject, 'http://purl.org/dc/terms/isVersionOf', 'http://example.com/foobarbaz');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_source_count_regex');

        $result = null;
        $this->assertCount(3, $t2['results']);
        foreach ($t2['results'] as $r) {
            if ($r['_id']['r'] == $subject) {
                $result = $r;
            }
        }

        $this->assertNotNull($result, 'Could not find table row for ' . $subject);
        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('source_count', $result, 'Result does not contain source_count');
        $this->assertEquals(3, $result['source_count']);
        $this->assertEquals(2, $result['regex_source_count']);
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testGenerateTableRowsWithCountOnJoinAndRegexUpdateAndRequery(): void
    {
        $this->tripodTables->generateTableRows('t_join_source_count_regex');

        $t1 = $this->tripodTables->getTableRows('t_join_source_count_regex', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        // expecting two rows
        $this->assertCount(1, $t1['results']);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count', $result, 'Result does not contain titles_count');
        $this->assertEquals(3, $result['titles_count']);

        // add a title to f340...
        $subjectGraph = $this->tripod->describeResource('http://jacs3.dataincubator.org/f340');
        $newGraph = new ExtendedGraph();
        $newGraph->add_graph($subjectGraph);
        $newGraph->add_resource_triple('http://jacs3.dataincubator.org/f340', 'http://purl.org/dc/terms/title', 'Another title');

        $this->tripod->saveChanges($subjectGraph, $newGraph);

        $t2 = $this->tripodTables->getTableRows('t_join_source_count_regex', ['_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);

        $this->assertCount(1, $t2['results']);
        $result = $t2['results'][0];

        // check out the columns
        $this->assertArrayHasKey('titles_count', $result, 'Result does not contain titles_count');
        $this->assertEquals(4, $result['titles_count']);
    }

    public function testUpdateWillDeleteItem(): void
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['deleteTableRowsForResource', 'generateTableRowsForType'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();
        $mockTables->expects($this->once())->method('deleteTableRowsForResource')->with('http://foo', 'context');
        $mockTables->expects($this->never())->method('generateTableRowsForType');

        $mockTables->update(new ImpactedSubject(['r' => 'http://foo', 'c' => 'context'], OP_TABLES, 'foo', 'bar', ['t_table']));
    }

    public function testUpdateWillGenerateRows(): void
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['deleteTableRowsForResource', 'generateTableRowsForResource'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();
        $mockTables->expects($this->once())->method('generateTableRowsForResource')->with('http://foo', 'context');
        $mockTables->expects($this->never())->method('deleteTableRowsForResource');

        $mockTables->update(new ImpactedSubject(['r' => 'http://foo', 'c' => 'context'], OP_TABLES, 'foo', 'bar', ['t_table']));
    }

    public function testGenerateTableRows(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource');

        // expecting two rows
        $this->assertCount(3, $t1['results']);
        $result = $t1['results'][0];

        // check out the columns
        $this->assertArrayHasKey('type', $result, 'Result does not contain type');
        $this->assertArrayHasKey('isbn', $result, 'Result does not contain isbn');
        $this->assertArrayHasKey('isbn13', $result, 'Result does not contain isbn13');
    }

    public function testTableRowUpsertIsRetriedOnDuplicateKeyError(): void
    {
        $collection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['updateOne', 'findOne'])
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->getMock();

        $invokedCount = $this->atLeast(2);
        $collection->expects($invokedCount)->method('updateOne')
            ->willReturnCallback(function ($filter, $doc, $options) use ($invokedCount) {
                static $prevFilter;
                static $prevDoc;

                // Fail the first time with a duplicate key error
                if ($invokedCount->getInvocationCount() === 1) {
                    $prevFilter = $filter;
                    $prevDoc = $doc;
                    $this->assertSame(['upsert' => true], $options);

                    throw new BulkWriteException('E11000 duplicate key error', 11000);
                }

                // On the second attempt, we should be trying to update the same document, but with upsert false
                if ($invokedCount->getInvocationCount() === 2) {
                    $this->assertSame($prevFilter, $filter);
                    $this->assertSame($prevDoc, $doc);
                    $this->assertSame(['upsert' => false], $options);
                }

                return $this->createMock(UpdateResult::class);
            });
        $collection->method('findOne')->willReturn(['_id' => [
            'r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
            'c' => 'http://talisaspire.com/',
            'type' => 't_resource',
            'value' => ['test' => 'data'],
        ]]);

        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForTable'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig(Config::getConfig());
        $configInstance->method('getCollectionForTable')->willReturn($collection);

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getConfigInstance'])
            ->setConstructorArgs([
                $this->tripod->getStoreName(),
                $this->getTripodCollection($this->tripod),
                'http://talisaspire.com/',
            ])
            ->getMock();
        $tables->method('getConfigInstance')->willReturn($configInstance);

        $tables->generateTableRows('t_resource');
    }

    public function testBatchTableRowGeneration(): void
    {
        $count = 234;
        $docs = [];

        $configOptions = $this->decodeJsonFile(__DIR__ . '/data/config.json');

        for ($i = 0; $i < $count; $i++) {
            $docs[] = ['_id' => ['r' => 'tenantLists:batch' . $i, 'c' => 'tenantContexts:DefaultGraph']];
        }

        $fakeCursor = new FakeCursor($docs);
        $configInstance = $this->getMockBuilder(TripodTestConfig::class)
            ->onlyMethods(['getCollectionForTable', 'getCollectionForCBD'])
            ->disableOriginalConstructor()
            ->getMock();
        $configInstance->loadConfig($configOptions);

        $collection = $this->getMockBuilder(Collection::class)
            ->onlyMethods(['count', 'find'])
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->getMock();
        $collection->expects($this->atLeastOnce())->method('count')->willReturn($count);
        $collection->expects($this->atLeastOnce())->method('find')->willReturn($fakeCursor);

        $configInstance->expects($this->atLeastOnce())->method('getCollectionForCBD')->willReturn($collection);

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getConfigInstance', 'queueApplyJob'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'tenantContexts:DefaultGraph'])
            ->getMock();
        $tables->expects($this->atLeastOnce())->method('getConfigInstance')->willReturn($configInstance);
        $tables->expects($this->exactly(3))->method('queueApplyJob')
            ->withConsecutive(
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(100)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ],
                [
                    $this->logicalAnd(
                        $this->isType('array'),
                        $this->containsOnlyInstancesOf(ImpactedSubject::class),
                        $this->countOf(34)
                    ),
                    'TESTQUEUE',
                    $this->isType('array'),
                ]
            );
        $tables->generateTableRows('t_resource', null, null, 'TESTQUEUE');
    }

    public function testGetTableRowsSort(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => -1, '_id.r' => 1]);
        // expecting two rows, first row should be one with highest numeric value of ISBN, due to sort DESC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2', $t1['results'][0]['_id']['r']);

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1, '_id.r' => 1]);

        // expecting two rows, first row should be one with lowest numeric value of ISBN, due to sort ASC
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsFilter(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', ['value.isbn' => '9780393929690']); // only bring back rows with isbn = 9780393929690

        // expecting one row
        $this->assertCount(1, $t1['results']);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);
    }

    public function testGetTableRowsLimitOffset(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $t1 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1], 0, 1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA
        $this->assertCount(1, $t1['results']);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA', $t1['results'][0]['_id']['r']);

        $t2 = $this->tripodTables->getTableRows('t_resource', [], ['value.isbn' => 1], 1, 1);

        // expecting http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2
        $this->assertCount(1, $t2['results']);
        $this->assertEquals('http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2', $t2['results'][0]['_id']['r']);
    }

    public function testGenerateTableRowsForResourceUnnamespaced(): void
    {
        $this->tripodTables->update(new ImpactedSubject(['r' => 'http://basedata.com/b/2', 'c' => 'http://basedata.com/b/DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');
    }

    public function testGenerateTableRowsForResourceNamespaced(): void
    {
        $this->tripodTables->update(new ImpactedSubject(['r' => 'baseData:2', 'c' => 'baseData:DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');
    }

    public function testGenerateTableRowsForResourceContextNamespaced(): void
    {
        $this->tripodTables->update(new ImpactedSubject(['r' => 'http://basedata.com/b/2', 'c' => 'baseData:DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');
    }

    public function testGenerateTableRowsForResourceResourceNamespaced(): void
    {
        $this->tripodTables->update(new ImpactedSubject(['r' => 'baseData:2', 'c' => 'http://basedata.com/b/DefaultGraph'], OP_TABLES, $this->tripodTables->getStoreName(), $this->tripodTables->getPodName(), ['t_work2']));

        $rows = $this->tripodTables->getTableRows('t_work2');

        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');
    }

    public function testGenerateTableRowsForResourcesOfTypeWithNamespace(): void
    {
        $mockTripodTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();
        $mockTripodTables->expects($this->atLeastOnce())
            ->method('generateTableRows')
            ->with('t_work2', 'http://example.com/1', 'http://talisaspire.com/', null)
            ->willReturn(['ok' => true]);

        // check where referred to as acorn:Work2 in spec...
        $mockTripodTables->generateTableRowsForType('http://talisaspire.com/schema#Work2', 'http://example.com/1', 'http://talisaspire.com/');

        $mockTripodTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([$this->tripod->getStoreName(), $this->getTripodCollection($this->tripod), 'http://talisaspire.com/'])
            ->getMock();
        $mockTripodTables->expects($this->atLeastOnce())->method('generateTableRows')->willReturn(['ok' => true]);

        // check where referred to as http://talisaspire.com/schema#Resource in spec...
        $mockTripodTables->generateTableRowsForType('acorn:Resource', 'http://example.com/2', 'http://talisaspire.com/');
    }

    /**
     * Test table specification predicate modifier config.
     */
    public function testGenerateTableRowsForUsersWithModifiersValidConfig(): void
    {
        $this->expectNotToPerformAssertions();

        // All config defined here should be valid
        $tableSpecifications = [
            _ID_KEY => 't_testGenerateTableRowsForUsersWithModifiersValidConfig',
            'fields' => [
                [
                    'fieldName' => 'test1',
                    'predicates' => [
                        'join' => [
                            'glue' => ';',
                            'predicates' => ['foaf:name'],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test2',
                    'predicates' => [
                        'lowercase' => [
                            'predicates' => ['foaf:name'],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test3',
                    'predicates' => [
                        'lowercase' => [
                            'join' => [
                                'glue' => ';',
                                'predicates' => ['foaf:name'],
                            ],
                        ],
                    ],
                ],
                [
                    'fieldName' => 'test4',
                    'predicates' => [
                        'date' => [
                            'predicates' => ['temp:last_login'],
                        ],
                    ],
                ],
            ],
        ];

        // Note that you need some config in order to create the Config object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        Config::setConfig($this->generateMongoTripodTestConfig());

        /** @var Tripod\Mongo\Config */
        $tripodConfig = Config::getInstance();

        foreach ($tableSpecifications['fields'] as $field) {
            // If there is invalid config, an exception will be thrown
            $tripodConfig->checkModifierFunctions($field['predicates'], Tripod\Mongo\Composites\Tables::$predicateModifiers);
        }
    }

    /**
     * Test invalid table specification predicate modifier config - use a bad attribute.
     */
    public function testGenerateTableRowsForUsersWithModifiersInvalidConfigBadGlue(): void
    {
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Invalid modifier: 'glue2' in key 'join'");

        // Create some dodgy config ("glue2") and see if an exception is thrown
        $tableSpecifications = [
            _ID_KEY => 't_foo',
            'fieldName' => 'test1',
            'predicates' => [
                'join' => [
                    'glue2' => ';',
                    'predicates' => ['foaf:name'],
                ],
            ],
        ];

        // Note that you need some config in order to create the Config object successfully.
        // Once that object has been created, we use our own table specifications to test against.
        Config::setConfig($this->generateMongoTripodTestConfig());

        /** @var Tripod\Mongo\Config */
        $tripodConfig = Config::getInstance();

        $tripodConfig->checkModifierFunctions($tableSpecifications['predicates'], Tripod\Mongo\Composites\Tables::$predicateModifiers);
    }

    /**
     * Test table rows have been generated successfully for a "join" modifier.
     */
    public function testGenerateTableRowsForUsersWithModifiersJoin(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        $this->assertEquals('Harry Potter', $rows['results'][0]['join']);
    }

    /**
     * Test table rows have been generated for a "join" modifier but with a single value rather than an array.
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinSingle(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        $this->assertEquals('Harry', $rows['results'][0]['joinSingle']);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier with a "join" inside it.
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCase(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        $this->assertEquals('harry potter', $rows['results'][0]['joinLowerCase']);
    }

    /**
     * Test table rows have been generated for a "date" modifier.
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDate(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        $this->assertInstanceOf(UTCDateTime::class, $rows['results'][0]['mongoDate']);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with a value that does not exist.
     */
    public function testGenerateTableRowsForUsersWithModifiersMongoDateDoesNotExist(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        // Test for data that doesn't exist
        $this->assertArrayNotHasKey('mongoDateDoesNotExist', $rows['results'][0]);
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier wtih a "join" modifier inside. It also has an
     * extra field attached to the row as well.
     */
    public function testGenerateTableRowsForUsersWithModifiersJoinLowerCaseAndExtraField(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        $this->assertArrayHasKey('joinLowerCaseANDExtraField', $rows['results'][0]);
        $this->assertIsArray($rows['results'][0]['joinLowerCaseANDExtraField']);
        $this->assertEquals('harry potter', $rows['results'][0]['joinLowerCaseANDExtraField'][0]);
        $this->assertEquals('Harry', $rows['results'][0]['joinLowerCaseANDExtraField'][1]);
    }

    /**
     * Test table rows have been generated for a "date" modifier but with an invalid date string.
     */
    public function testGenerateTableRowsForUsersWithModifiersDateInvalid(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        // Check borked data
        // Trying to use date but passed in a string - should default to 0 for sec and usec
        $this->assertInstanceOf(UTCDateTime::class, $rows['results'][0]['mongoDateInvalid']);
        $this->assertEquals(0, $rows['results'][0]['mongoDateInvalid']->__toString());
    }

    /**
     * Test table rows have been generated for a "lowercase" modifier around a "date" modifier.
     */
    public function testGenerateTableRowsForUsersWithModifiersLowercaseDate(): void
    {
        // Get table rows
        $rows = $this->generateTableRows('t_users');

        // We should have 1 result and it should have modified fields
        $this->assertEquals(1, $rows['head']['count'], 'Expected one row');

        // Lowercasing a mongodate object should be the same as running a __toString() on the date object
        $this->assertEquals($rows['results'][0]['mongoDate']->__toString(), $rows['results'][0]['lowercaseDate']);
    }

    /**
     * Test table rows are tuncated if they are too large to index.
     */
    public function testGenerateTableRowsTruncatesFieldsTooLargeToIndex(): void
    {
        $indexUtils = new IndexUtils();
        $indexUtils->ensureIndexes(false, $this->tripod->getStoreName(), false);

        $fullTitle = 'Mahommah Gardo Baquaqua. Biography of Mahommah G. Baquaqua, a Native of Zoogoo, in the Interior of Africa. (A Convert to Christianity,) With a Description of That Part of the World; Including the Manners and Customs of the Inhabitants, Their Religious Notions, Form of Government, Laws, Appearance of the Country, Buildings, Agriculture, Manufactures, Shepherds and Herdsmen, Domestic Animals, Marriage Ceremonials, Funeral Services, Styles of Dress, Trade and Commerce, Modes of Warfare, System of Slavery, &amp;c., &amp;c. Mahommah&#039;s Early Life, His Education, His Capture and Slavery in Western Africa and Brazil, His Escape to the United States, from Thence to Hayti, (the City of Port Au Prince,) His Reception by the Baptist Missionary There, The Rev. W. L. Judd; His Conversion to Christianity, Baptism, and Return to This Country, His Views, Objects and Aim. Written and Revised from His Own Words, by Samuel Moore, Esq., Late Publisher of the &quot;North of England Shipping Gazette,&quot; Author of Several Popular Works, and Editor of Sundry Reform Papers.';
        $truncatedTitle = substr($fullTitle, 0, 1007); // 1007 = 1024 - index name "value_title_1" + Randomness
        $fullTitleLength = strlen($fullTitle);
        $truncatedTitleLength = strlen($truncatedTitle);

        $rows = $this->generateTableRows('t_truncation');

        // When using Mongo 2.4 and below, the string will not have been truncated.
        // Due to stricter index key enforcement in Mongo 2.6 and above, the string will have been truncated.
        // Allow the test to pass for either version of Mongo
        $actualLength = strlen($rows['results'][0]['title']);
        $this->assertTrue($actualLength === $fullTitleLength || $actualLength === $truncatedTitleLength, 'Title is an unexpected length');

        // Assert that the title starts with the truncated title.
        // This will be the case for both Mongo 2.4 and Mongo 2.6
        $this->assertSame(0, strpos($rows['results'][0]['title'], $truncatedTitle), 'Unexpected title');
    }

    /**
     * Test that link modifier is derived from the joined resource id, rather than base.
     */
    public function testJoinLinkValueIsForJoinedResource(): void
    {
        $this->tripodTables->generateTableRows('t_join_link');
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:foo1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertArrayHasKey('knowsLink', $rows['results'][0]);
        $this->assertArrayHasKey('workLink', $rows['results'][0]);
        // Check bookLink values
        $this->assertEquals('baseData:foo1234', $rows['results'][0]['_id']['r']);
        $this->assertEquals('http://basedata.com/b/foo1234', $rows['results'][0]['bookLink']);

        // Check authorLink values
        $this->assertEquals('user:10101', $rows['results'][0]['authorUri']);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10101', $rows['results'][0]['authorLink']);

        // Check knowsLink values
        $this->assertEquals('user:10102', $rows['results'][0]['knowsUri']);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10102', $rows['results'][0]['knowsLink']);

        // Check workLink values
        $this->assertEquals('http://talisaspire.com/works/4d101f63c10a6', $rows['results'][0]['workUri']); // Already a fq URI
        $this->assertEquals('http://talisaspire.com/works/4d101f63c10a6', $rows['results'][0]['workLink']);
    }

    /**
     * Test to ensure that impact index contains joined ids for resources that do not yet exist in the database (i.e.
     * allow open world model).
     */
    public function testPreviouslyUnavailableDataBecomesPresentAndTriggersTableRegen(): void
    {
        $this->tripodTables->generateTableRows('t_join_link');
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:bar1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertEquals('user:10103', $rows['results'][0]['authorUri']);
        // Author link should not appear because resource has not yet been created
        $this->assertArrayNotHasKey('authorLink', $rows['results'][0]);

        $uri = 'http://schemas.talis.com/2005/user/schema#10103';
        // Confirm this user does not exist
        $this->assertFalse($this->tripod->describeResource($uri)->has_triples_about($uri));

        $g = new MongoGraph();
        $g->add_resource_triple($uri, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('foaf:Person'));
        $g->add_literal_triple($uri, $g->qname_to_uri('foaf:name'), 'A. Nonymous');

        $this->tripod->saveChanges(new MongoGraph(), $g, 'http://talisaspire.com/', "This resource didn't exist at join time");

        $userGraph = $this->tripod->describeResource($uri);

        $this->assertTrue($userGraph->has_triples_about($uri), 'new entity we created was not saved');

        // Get our table rows again
        $rows = $this->tripodTables->getTableRows('t_join_link', ['_id.r' => 'baseData:bar1234']);
        // authorLink should now be populated
        $this->assertArrayHasKey('authorLink', $rows['results'][0]);
        $this->assertEquals($uri, $rows['results'][0]['authorLink']);
    }

    /**
     * Ensure that an array of links is returned if there are multiple resources matched by the join.
     */
    public function testLinkWorksOnRepeatingPredicatesForResource(): void
    {
        $this->tripodTables->generateTableRows('t_link_multiple');
        $rows = $this->tripodTables->getTableRows('t_link_multiple', ['_id.r' => 'baseData:bar1234']);
        $this->assertEquals(1, $rows['head']['count']);
        $this->assertArrayHasKey('contributorLink', $rows['results'][0]);
        $this->assertTrue(is_array($rows['results'][0]['contributorLink']));
        $this->assertCount(2, $rows['results'][0]['contributorLink']);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10101', $rows['results'][0]['contributorLink'][0]);
        $this->assertEquals('http://schemas.talis.com/2005/user/schema#10102', $rows['results'][0]['contributorLink'][1]);
    }

    /**
     * Return the distinct values of a table column.
     */
    public function testDistinct(): void
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $rows = $this->tripodTables->getTableRows($table, [], [], 0, 0);
        $this->assertEquals(11, $rows['head']['count']);
        $results = $this->tripodTables->distinct($table, 'value.title');

        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(4, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(4, $results['results']);
        $this->assertContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        // Supply a filter
        $results = $this->tripodTables->distinct($table, 'value.title', ['value.type' => 'bibo:Document']);
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(2, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(2, $results['results']);
        $this->assertNotContains('Physics 3rd Edition: Physics for Engineers and Scientists', $results['results']);
        $this->assertContains('A document title', $results['results']);
        $this->assertContains('Another document title', $results['results']);

        $results = $this->tripodTables->distinct($table, 'value.type');
        $this->assertArrayHasKey('head', $results);
        $this->assertArrayHasKey('count', $results['head']);
        $this->assertEquals(7, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertCount(7, $results['results']);
        $this->assertContains('acorn:Resource', $results['results']);
        $this->assertContains('acorn:Work', $results['results']);
        $this->assertContains('bibo:Book', $results['results']);
        $this->assertContains('bibo:Document', $results['results']);
    }

    /**
     * Return no results for tablespec that doesn't exist.
     */
    public function testDistinctOnTableSpecThatDoesNotExist(): void
    {
        $table = 't_nothing_to_see_here';
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage("Table id 't_nothing_to_see_here' not in configuration");
        $this->tripodTables->distinct($table, 'value.foo');
    }

    /**
     * Return no results for distinct on a fieldname that is not defined in tableSpec.
     */
    public function testDistinctOnFieldNameThatIsNotInTableSpec(): void
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, 'value.foo');
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    /**
     * Return no results for filters that match no table rows.
     */
    public function testDistinctForFilterWithNoMatches(): void
    {
        // Get table rows
        $table = 't_distinct';
        $this->generateTableRows($table);
        $results = $this->tripodTables->distinct($table, 'value.title', ['value.foo' => 'wibble']);
        $this->assertEquals(0, $results['head']['count']);
        $this->assertArrayHasKey('results', $results);
        $this->assertEmpty($results['results']);
    }

    public function testTableRowsGenerateWhenDefinedPredicateChanges(): void
    {
        foreach (array_keys(Config::getInstance()->getTableSpecifications($this->tripod->getStoreName())) as $specId) {
            $this->generateTableRows($specId);
        }

        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2';

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    'async' => [
                        OP_VIEWS => true,
                        OP_TABLES => false,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $labeller = new Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:title'],
        ];

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs([
                $tripod->getStoreName(),
                $this->getTripodCollection($tripod),
                'http://talisaspire.com/',
            ])
            ->getMock();

        $tables->expects($this->exactly(2))
            ->method('generateTableRows')
            ->withConsecutive(
                [
                    $this->equalTo('t_distinct'),
                    $this->equalTo($uri),
                    $this->equalTo($this->defaultContext),
                ],
                [
                    $this->equalTo('t_join_source_count_regex'),
                    $this->equalTo($uri),
                    $this->equalTo($this->defaultContext),
                ]
            );

        $tripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->willReturn($tables);

        // Walk through the processSyncOperations process manually for tables

        $table = $tripod->getComposite(OP_TABLES);
        $this->assertInstanceOf(Tripod\Mongo\Composites\Tables::class, $table);

        $expectedImpactedSubject = new ImpactedSubject(
            [
                _ID_RESOURCE => $labeller->uri_to_alias($uri),
                _ID_CONTEXT => $this->defaultContext,
            ],
            OP_TABLES,
            $this->defaultStoreName,
            $this->defaultPodName,
            ['t_distinct', 't_join_source_count_regex']
        );

        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $impactedSubject = $impactedSubjects[0];

        $this->assertEquals($expectedImpactedSubject->getResourceId(), $impactedSubject->getResourceId());
        $this->assertEquals($expectedImpactedSubject->getOperation(), $impactedSubject->getOperation());
        $this->assertEquals($expectedImpactedSubject->getStoreName(), $impactedSubject->getStoreName());
        $this->assertEquals($expectedImpactedSubject->getPodName(), $impactedSubject->getPodName());

        // Order of these doesn't matter - so sort the spec types for matching
        $expectedSpecTypes = $expectedImpactedSubject->getSpecTypes();
        sort($expectedSpecTypes);
        $specTypes = $impactedSubject->getSpecTypes();
        sort($specTypes);

        $this->assertEquals($expectedSpecTypes, $specTypes);

        foreach ($impactedSubjects as $subject) {
            $table->update($subject);
        }

        // This should be 0, because we mocked the actual adding of the regenerated table.  If it's zero, however,
        // it means we successfully deleted the views with $uri1 in the impactIndex
        $collections = Config::getInstance()->getCollectionsForTables($this->defaultStoreName);
        foreach ($collections as $collection) {
            $query = [
                'value._impactIndex' => ['r' => $labeller->uri_to_alias($uri), 'c' => $this->defaultContext],
                '_id.type' => ['$in' => ['t_distinct', 't_join_source_count_regex']],
            ];
            $this->assertEquals(0, $collection->count($query));
        }
    }

    public function testTableRowsNotGeneratedWhenUndefinedPredicateChanges(): void
    {
        foreach (array_keys(Config::getInstance()->getTableSpecifications($this->tripod->getStoreName())) as $specId) {
            $this->generateTableRows($specId);
        }

        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2';

        $labeller = new Labeller();
        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => ['dct:description'],
        ];

        // Walk through the processSyncOperations process manually for tables

        $table = new Tripod\Mongo\Composites\Tables(
            $this->defaultStoreName,
            Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
            $this->defaultContext
        );

        $impactedSubjects = $table->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEmpty($impactedSubjects);
    }

    public function testUpdateOfResourceInImpactIndexTriggersRegenerationTableRows(): void
    {
        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRows'])
            ->setConstructorArgs($this->tablesConstParams)
            ->getMock();

        $mockTables->expects($this->exactly(2))
            ->method('generateTableRows')
            ->withConsecutive(
                [
                    't_resource',
                    'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                    $this->defaultContext,
                ],
                [
                    't_resource',
                    'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',
                    $this->defaultContext,
                ]
            );

        $labeller = new Labeller();
        // generate table rows
        $this->tripodTables->generateTableRows('t_resource');

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias('http://talisaspire.com/works/4d101f63c10a6') => [
                'bibo:isbn13',
            ],
        ];

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA',
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_resource']
            ),
            new ImpactedSubject(
                [
                    _ID_RESOURCE => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2',
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_resource']
            ),
        ];

        $impactedSubjects = $mockTables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);

        // next, trigger regen for work we know is associated with 2x resources. Should trigger view regen for resources
        foreach ($impactedSubjects as $subject) {
            $mockTables->update($subject);
        }
    }

    public function testRdfTypeTriggersGenerationOfTableRows(): void
    {
        $uri = 'http://example.com/resources/' . uniqid();

        $labeller = new Labeller();
        $graph = new ExtendedGraph();
        // This should trigger a table row regeneration, even though issn isn't in the tablespec
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('acorn:Resource'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('bibo:issn'), '1234-5678');

        $subjectsAndPredicatesOfChange = [
            $labeller->uri_to_alias($uri) => [
                'rdf:type',
                'bibo:issn',
            ],
        ];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['generateTableRowsForType'])
            ->setConstructorArgs([
                $this->defaultStoreName,
                Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
                $this->defaultContext,
            ])
            ->getMock();

        $mockTables->expects($this->once())
            ->method('generateTableRowsForType')
            ->with(
                'acorn:Resource',
                $labeller->uri_to_alias($uri),
                $this->defaultContext,
                []
            );

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $labeller->uri_to_alias($uri),
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                []
            ),
        ];

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $impactedSubjects = $mockTables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);

        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
        foreach ($impactedSubjects as $subject) {
            $mockTables->update($subject);
        }
    }

    public function testUpdateToResourceWithMatchingRdfTypeShouldOnlyRegenerateIfRdfTypeIsPartOfUpdate(): void
    {
        $uri = 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA';
        $labeller = new Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $tables = new Tripod\Mongo\Composites\Tables(
            $this->defaultStoreName,
            Config::getInstance()->getCollectionForCBD($this->defaultStoreName, $this->defaultPodName),
            $this->defaultContext
        );

        $subjectsAndPredicatesOfChange = [$uriAlias => ['dct:subject']];

        $this->assertEmpty($tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));

        $subjectsAndPredicatesOfChange = [$uriAlias => ['dct:subject', 'rdf:type']];

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                []
            ),
        ];

        $impactedSubjects = $tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext);
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testNewResourceThatDoesNotMatchAnythingCreatesNoImpactedSubjects(): void
    {
        $uri = 'http://example.com/resources/' . uniqid();
        $labeller = new Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new ExtendedGraph();
        $graph->add_resource_triple($uri, RDF_TYPE, $labeller->qname_to_uri('bibo:Proceedings'));
        $graph->add_literal_triple($uri, $labeller->qname_to_uri('dct:title'), 'A title');

        $subjectsAndPredicatesOfChange = [$uriAlias => ['rdf:type', 'dct:title']];

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods([
                'getDataUpdater',
            ])
            ->setConstructorArgs([
                $this->defaultPodName,
                $this->defaultStoreName,
                [
                    'defaultContext' => $this->defaultContext,
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([
                'processSyncOperations',
                'queueAsyncOperations',
            ])
            ->setConstructorArgs([
                $mockTripod,
                [
                    OP_ASYNC => [
                        OP_TABLES => false,
                        OP_VIEWS => true,
                        OP_SEARCH => true,
                    ],
                ],
            ])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripodUpdates->expects($this->once())
            ->method('queueAsyncOperations')
            ->with(
                $subjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $mockTripod->saveChanges(new ExtendedGraph(), $graph);

        $tables = $mockTripod->getComposite(OP_TABLES);

        $this->assertEmpty($tables->getImpactedSubjects($subjectsAndPredicatesOfChange, $this->defaultContext));
    }

    public function testDeleteResourceCreatesImpactedSubjects(): void
    {
        $uri = 'http://example.com/users/' . uniqid();
        $labeller = new Labeller();
        $uriAlias = $labeller->uri_to_alias($uri);

        $graph = new ExtendedGraph();
        $graph->add_resource_triple(
            $uri,
            RDF_TYPE,
            $labeller->qname_to_uri('spec:User')
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('foaf:firstName'),
            'Anne'
        );
        $graph->add_literal_triple(
            $uri,
            $labeller->qname_to_uri('foaf:surname'),
            'Onymous'
        );

        $uri2 = 'http://example.com/users/' . uniqid();
        $uriAlias2 = $labeller->uri_to_alias($uri2);

        $graph2 = new ExtendedGraph();
        $graph2->add_resource_triple(
            $uri2,
            RDF_TYPE,
            $labeller->qname_to_uri('spec:User')
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('foaf:firstName'),
            'Ann'
        );
        $graph2->add_literal_triple(
            $uri2,
            $labeller->qname_to_uri('foaf:surname'),
            "O'ther"
        );

        // Save the graphs and ensure that table rows are generated
        $tripod = new Driver(
            $this->defaultPodName,
            $this->defaultStoreName,
            [
                'defaultContext' => $this->defaultContext,
                OP_ASYNC => [
                    OP_VIEWS => false,
                    OP_TABLES => false,
                    OP_SEARCH => false,
                ],
            ]
        );

        $tripod->saveChanges(new ExtendedGraph(), $graph);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(1, $tableRows['head']['count']);

        $tripod->saveChanges(new ExtendedGraph(), $graph2);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias2,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(1, $tableRows['head']['count']);

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    $this->defaultPodName,
                    $this->defaultStoreName,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->getMock();

        $mockTripodUpdates = $this->getMockBuilder(Updates::class)
            ->setConstructorArgs(
                [
                    $mockTripod,
                    [
                        'defaultContext' => $this->defaultContext,
                        OP_ASYNC => [
                            OP_VIEWS => false,
                            OP_TABLES => false,
                            OP_SEARCH => false,
                        ],
                    ],
                ]
            )->onlyMethods(['processSyncOperations'])
            ->getMock();

        $mockTripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($mockTripodUpdates);

        $expectedSubjectsAndPredicatesOfChange = [
            $uriAlias => ['rdf:type', 'foaf:firstName', 'foaf:surname'],
            $uriAlias2 => ['rdf:type', 'foaf:firstName', 'foaf:surname'],
        ];

        $mockTripodUpdates->expects($this->once())
            ->method('processSyncOperations')
            ->with(
                $expectedSubjectsAndPredicatesOfChange,
                $this->defaultContext
            );

        $graph->add_graph($graph2);

        // Delete both user resources
        $mockTripod->saveChanges($graph, new ExtendedGraph());

        $deletedGraph = $mockTripod->describeResources([$uri, $uri2]);

        $this->assertTrue($deletedGraph->is_empty());

        // Manually walk through the tables operation
        /** @var Tripod\Mongo\Composites\Tables $tables */
        $tables = $mockTripod->getComposite(OP_TABLES);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_users']
            ),
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $uriAlias2,
                    _ID_CONTEXT => $this->defaultContext,
                ],
                OP_TABLES,
                $this->defaultStoreName,
                $this->defaultPodName,
                ['t_users']
            ),
        ];

        $this->assertEquals($expectedImpactedSubjects, $tables->getImpactedSubjects($expectedSubjectsAndPredicatesOfChange, $this->defaultContext));

        foreach ($expectedImpactedSubjects as $subject) {
            $tables->update($subject);
        }

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(0, $tableRows['head']['count']);

        $tableRows = $tripod->getTableRows(
            't_users',
            [
                _ID_KEY . '.' . _ID_RESOURCE => $uriAlias2,
                _ID_KEY . '.' . _ID_CONTEXT => $this->defaultContext,
            ]
        );

        $this->assertEquals(0, $tableRows['head']['count']);
    }

    /**
     * Save several new resources in a single operation. Only one of the resources has a type that is applicable based on specifications,
     * therefore only one ImpactedSubject should be created.
     */
    public function testSavingMultipleNewEntitiesResultsInOneImpactedSubject(): void
    {
        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                [
                    'CBD_testing',
                    'tripod_php_testing',
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripodUpdates = $this->getMockBuilder(Updates::class)
            ->onlyMethods([])
            ->setConstructorArgs(
                [
                    $tripod,
                    [
                        'defaultContext' => 'http://talisaspire.com/',
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => true,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )->getMock();

        $tripod->expects($this->once())
            ->method('getDataUpdater')
            ->willReturn($tripodUpdates);

        // first lets add a book, which should trigger a search doc, view and table gen for a single item
        $g = new MongoGraph();
        $newSubjectUri1 = 'http://talisaspire.com/resources/newdoc1';
        $newSubjectUri2 = 'http://talisaspire.com/resources/newdoc2';
        $newSubjectUri3 = 'http://talisaspire.com/resources/newdoc3';

        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Article')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri1, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:title'), 'This is a new resource');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'history');
        $g->add_literal_triple($newSubjectUri1, $g->qname_to_uri('dct:subject'), 'philosophy');

        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Book')); // this is the only resource that should be queued
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('acorn:Resource'));
        $g->add_resource_triple($newSubjectUri2, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:title'), 'This is another new resource');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'maths');
        $g->add_literal_triple($newSubjectUri2, $g->qname_to_uri('dct:subject'), 'science');

        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('rdf:type'), $g->qname_to_uri('bibo:Journal')); // there are no specs that are applicable for this type alone
        $g->add_resource_triple($newSubjectUri3, $g->qname_to_uri('dct:creator'), 'http://talisaspire.com/authors/1');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:title'), 'This is yet another new resource');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'art');
        $g->add_literal_triple($newSubjectUri3, $g->qname_to_uri('dct:subject'), 'design');

        $subjectsAndPredicatesOfChange = [
            $newSubjectUri1 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri2 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
            $newSubjectUri3 => ['rdf:type', 'dct:creator', 'dct:title', 'dct:subject'],
        ];
        $tripod->saveChanges(new MongoGraph(), $g);

        /** @var Tripod\Mongo\Composites\Tables $tables */
        $tables = $tripod->getComposite(OP_TABLES);

        $expectedImpactedSubjects = [
            new ImpactedSubject(
                [
                    _ID_RESOURCE => $newSubjectUri2,
                    _ID_CONTEXT => 'http://talisaspire.com/',
                ],
                OP_TABLES,
                'tripod_php_testing',
                'CBD_testing',
                []
            ),
        ];

        $impactedSubjects = $tables->getImpactedSubjects($subjectsAndPredicatesOfChange, 'http://talisaspire.com/');
        $this->assertEquals($expectedImpactedSubjects, $impactedSubjects);
    }

    public function testRemoveTableSpecDoesNotAffectInvalidation(): void
    {
        foreach (array_keys(Config::getInstance()->getTableSpecifications($this->tripod->getStoreName())) as $specId) {
            $this->generateTableRows($specId);
        }

        $context = 'http://talisaspire.com/';
        $uri = 'http://talisaspire.com/works/4d101f63c10a6';

        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_resource');
        $this->assertGreaterThan(0, $collection->count(['_id.type' => 't_resource', 'value._impactIndex' => [_ID_RESOURCE => $uri, _ID_CONTEXT => $context]]));
        $config = Config::getConfig();
        unset($config['stores']['tripod_php_testing']['table_specifications'][0]);
        Config::setConfig($config);

        $mockTripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(
                [
                    'CBD_testing',
                    'tripod_php_testing',
                    [
                        'defaultContext' => $context,
                        OP_ASYNC => [
                            OP_VIEWS => true,
                            OP_TABLES => false,
                            OP_SEARCH => true,
                        ],
                    ],
                ]
            )
            ->getMock();

        $mockTables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['update'])
            ->setConstructorArgs(
                [
                    'tripod_php_testing',
                    Config::getInstance()->getCollectionForCBD('tripod_php_testing', 'CBD_testing'),
                    $context,
                ]
            )
            ->getMock();

        $labeller = new Labeller();

        $mockTripod->expects($this->once())
            ->method('getComposite')
            ->with(OP_TABLES)
            ->willReturn($mockTables);

        $mockTables->expects($this->never())
            ->method('update');

        $originalGraph = $mockTripod->describeResource($uri);
        $updatedGraph = $originalGraph->get_subject_subgraph($uri);
        $updatedGraph->add_literal_triple($uri, $labeller->qname_to_uri('dct:description'), 'Physics textbook');

        $mockTripod->saveChanges($originalGraph, $updatedGraph);

        // The table row should still be there, even if the tablespec no longer exists
        $this->assertGreaterThan(0, $collection->count(['_id.type' => 't_resource', 'value._impactIndex' => [_ID_RESOURCE => $uri, _ID_CONTEXT => $context]]));
    }

    public function testCountTables(): void
    {
        $collection = $this->getMockBuilder(Collection::class)
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->willReturn($collection);

        $collection->expects($this->once())
            ->method('count')
            ->with(['_id.type' => 't_source_count'])
            ->willReturn(50);

        $this->assertEquals(50, $tables->count('t_source_count'));
    }

    public function testCountTablesWithFilters(): void
    {
        $filters = ['_cts' => ['$lte' => new UTCDateTime()]];
        $query = array_merge(['_id.type' => 't_source_count'], $filters);
        $collection = $this->getMockBuilder(Collection::class)
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->onlyMethods(['count'])
            ->getMock();
        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->willReturn($collection);

        $collection->expects($this->once())
            ->method('count')
            ->with($query)
            ->willReturn(37);

        $this->assertEquals(37, $tables->count('t_source_count', $filters));
    }

    public function testDeleteTableRowsByTableId(): void
    {
        $collection = $this->getMockBuilder(Collection::class)
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->willReturn(2);

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->willReturn($collection);

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with(['_id.type' => 't_source_count'])
            ->willReturn($deleteResult);

        $this->assertEquals(2, $tables->deleteTableRowsByTableId('t_source_count'));
    }

    public function testDeleteTableRowsByTableIdWithTimestamp(): void
    {
        $timestamp = new UTCDateTime();

        $query = [
            '_id.type' => 't_source_count',
            '$or' => [
                [\_CREATED_TS => ['$lt' => $timestamp]],
                [\_CREATED_TS => ['$exists' => false]],
            ],
        ];
        $collection = $this->getMockBuilder(Collection::class)
            ->setConstructorArgs([new Manager(), 'db', 'coll'])
            ->onlyMethods(['deleteMany'])
            ->getMock();

        $deleteResult = $this->getMockBuilder(DeleteResult::class)
            ->onlyMethods(['getDeletedCount'])
            ->disableOriginalConstructor()
            ->getMock();

        $deleteResult->expects($this->once())
            ->method('getDeletedCount')
            ->willReturn(11);

        $tables = $this->getMockBuilder(Tripod\Mongo\Composites\Tables::class)
            ->onlyMethods(['getCollectionForTableSpec'])
            ->setConstructorArgs(['tripod_php_testing', $collection, 'http://example.com/'])
            ->getMock();

        $tables->expects($this->once())
            ->method('getCollectionForTableSpec')
            ->with('t_source_count')
            ->willReturn($collection);

        $collection->expects($this->once())
            ->method('deleteMany')
            ->with($query)
            ->willReturn($deleteResult);

        $this->assertEquals(11, $tables->deleteTableRowsByTableId('t_source_count', $timestamp));
    }

    public function testTablesDocuments(): void
    {
        $dbDoc = [
            '_id' => [
                'r' => 'http://talis.com/modules/xmen-004',
                'c' => 'tenantContexts:DefaultGraph',
                'type' => 't_report_hierarchy',
            ],
            '_cts' => new UTCDateTime(1535454036),
            'value' => [
                '_id' => [
                    'r' => 'http://talis.com/modules/xmen-004',
                    'c' => 'tenantContexts:DefaultGraph',
                ],
                '_impactIndex' => [
                    ['r' => 'http://talis.com/modules/xmen-004', 'c' => 'tenantContexts:DefaultGraph'],
                    ['r' => 'http://talis.com/schools/xmen-001', 'c' => 'tenantContexts:DefaultGraph'],
                ],
                'code' => 'XMEN-004',
                'nodeUrl' => 'http://talis.com/modules/xmen-004',
                'name' => 'Psychology: Living with The Voices',
                'description' => "Professor Deadpool will attempt to give you the ability to embrace your mental disorder and use it to your advantage. Whether you suffer from Schizophrenia, Dissociative Identity Disorder or plain old Comic Awareness, Wade Wilson has probably suffered through it himself. And while Deadpool can't solve your problem, he can teach you how to make it one of your most marketable qualities.",
                'parentCode' => 'XMEN-001',
                'parentNodeUrl' => 'http://talis.com/schools/xmen-001',
                'listCount' => 0,
                'type' => 'Module',
                'hasLinkedLists' => 'false',
            ],
        ];
        $doc = new Tables();
        $this->assertEquals([], $doc->getArrayCopy());
        $doc = new Tables($dbDoc);
        $this->assertEquals('XMEN-004', $doc['code']);
        $id = $doc['_id'];
        $this->assertIsArray($id);
        $this->assertEquals('http://talis.com/modules/xmen-004', $id['r']);
        $this->assertArrayNotHasKey('_cts', $doc);
        $this->assertArrayNotHasKey('_impactIndex', $doc);
        $this->assertArrayNotHasKey('type', $id);
    }

    public function testGetTableRowsNoCount(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $tableRows = $this->tripodTables->getTableRows('t_resource', [], [], 0, 1, ['includeCount' => false]);

        $this->assertCount(1, $tableRows['results']);
        $this->assertEquals(-1, $tableRows['head']['count']);
    }

    public function testGetTableRowsReturnCursor(): void
    {
        $this->tripodTables->generateTableRows('t_resource');

        $tableRows = $this->tripodTables->getTableRows('t_resource', [], [], 0, 1, ['returnCursor' => true]);

        $this->assertInstanceOf(Cursor::class, $tableRows['results']);
        $count = 0;
        foreach ($tableRows['results'] as $result) {
            $this->assertInstanceOf(Tables::class, $result);
            $count++;
        }

        $this->assertSame(1, $count);
        $this->assertGreaterThan(1, $tableRows['head']['count']);
    }

    /**
     * Generate dummy config that we can use for creating a Config object.
     *
     * @return array<string, non-empty-array|string>
     */
    private function generateMongoTripodTestConfig(): array
    {
        return ['defaultContext' => 'http://talisaspire.com/', 'data_sources' => [
            'db' => [
                'type' => 'mongo',
                'connection' => 'mongodb://localhost',
            ],
            'tlog' => [
                'type' => 'mongo',
                'connection' => 'mongodb://tloghost:27017,tloghost:27018',
            ],
        ], 'stores' => [
            $this->defaultStoreName => [
                'data_source' => 'db',
                'pods' => [
                    $this->defaultPodName => [],
                ],
            ],
        ], 'queue' => ['database' => 'queue', 'collection' => 'q_queue', 'data_source' => 'db'], 'transaction_log' => [
            'database' => 'transactions',
            'collection' => 'transaction_log',
            'data_source' => 'db',
        ]];
    }

    /**
     * Generate table rows based off an id.
     */
    private function generateTableRows(string $id): array
    {
        $this->tripodTables->generateTableRows($id);

        return $this->tripodTables->getTableRows($id);
    }
}
