<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\Mongo\Driver;

class MongoTripodComputedFieldsTest extends MongoTripodTestBase
{
    private array $originalConfig = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->originalConfig = Config::getConfig();
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MAX);
    }

    protected function tearDown(): void
    {
        Config::setConfig($this->originalConfig);
        Tripod\Mongo\Config::setValidationLevel(Tripod\Mongo\Config::VALIDATE_MIN);
        parent::tearDown();
    }

    public function testConditionalComputedFieldWithDates(): void
    {
        $tableSpec = [
            '_id' => 't_conditional_creators',
            'type' => ['bibo:Document'],
            'from' => 'CBD_testing',
            'fields' => [
                [
                    'fieldName' => 'dateUpdated',
                    'predicates' => [[
                        'date' => [
                            'predicates' => ['dct:updated'],
                        ],
                    ]],
                ],
                [
                    'fieldName' => 'datePublished',
                    'predicates' => [[
                        'date' => [
                            'predicates' => ['dct:published'],
                        ],
                    ]],
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'status',
                    'value' => [
                        'conditional' => [
                            'if' => ['$dateUpdated', '>', '$datePublished'],
                            'then' => 'Updated',
                            'else' => 'Published',
                        ],
                    ],
                ],
            ],
        ];

        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadDatesDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');

        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:foo1234']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Updated', $tableDoc['value']['status']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:foo12345']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Published', $tableDoc['value']['status']);

        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testConditionalComputedField(): void
    {
        $tableSpec = [
            '_id' => 't_conditional_creators',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'counts' => [
                [
                    'fieldName' => 'creatorCount',
                    'property' => 'dct:creator',
                ],
                [
                    'fieldName' => 'contributorCount',
                    'property' => 'dct:contributor',
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'creatorCount',
                    'value' => [
                        'conditional' => [
                            'if' => ['$creatorCount'],
                            'then' => '$creatorCount',
                            'else' => 1234,
                        ],
                    ],
                ],
                [
                    'fieldName' => 'contributorCount',
                    'value' => [
                        'conditional' => [
                            'if' => ['$contributorCount'],
                            'then' => '$contributorCount',
                            'else' => 1234,
                        ],
                    ],
                ],
            ],
        ];

        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:foo1234']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(1234, $tableDoc['value']['contributorCount']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:bar1234']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(2, $tableDoc['value']['contributorCount']);

        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testNestedConditionalComputedField(): void
    {
        $tableSpec = [
            '_id' => 't_conditional_creators',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'counts' => [
                [
                    'fieldName' => 'creatorCount',
                    'property' => 'dct:creator',
                    'temporary' => true,
                ],
                [
                    'fieldName' => 'contributorCount',
                    'property' => 'dct:contributor',
                    'temporary' => true,
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'normalizedCreatorCount',
                    'value' => [
                        'conditional' => [
                            'if' => ['$contributorCount'],
                            'then' => '$contributorCount',
                            'else' => [
                                'conditional' => [
                                    'if' => ['$creatorCount'],
                                    'then' => '$creatorCount',
                                    'else' => 'NO CONTRIBUTORS FOUND',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'] = [$tableSpec];
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:foo1234']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals(1, $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'baseData:bar1234']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(2, $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('NO CONTRIBUTORS FOUND', $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('NO CONTRIBUTORS FOUND', $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('NO CONTRIBUTORS FOUND', $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_creators', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('NO CONTRIBUTORS FOUND', $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testReplaceComputedField(): void
    {
        $tableSpec = [
            '_id' => 't_replace_type',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'fields' => [
                [
                    'fieldName' => 'rdfType',
                    'predicates' => [[
                        'join' => ['glue' => ' ', 'predicates' => ['rdf:type']],
                    ]],
                    'temporary' => true,
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'resourceType',
                    'value' => [
                        'replace' => [
                            'search' => 'bibo:',
                            'replace' => '',
                            'subject' => '$rdfType',
                        ],
                    ],
                ],
                [
                    'fieldName' => 'resourceType',
                    'value' => [
                        'replace' => [
                            'search' => 'acorn:',
                            'replace' => '',
                            'subject' => '$resourceType',
                        ],
                    ],
                ],
            ],
        ];

        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_replace_type');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_replace_type');

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'baseData:foo1234']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'baseData:bar1234']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Book Resource Testing', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Book Resource', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_replace_type', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testArithmeticComputedField(): void
    {
        $tableSpec = [
            '_id' => 't_creator_count',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'counts' => [
                [
                    'fieldName' => 'creatorCount',
                    'property' => 'dct:creator',
                    'temporary' => true,
                ],
                [
                    'fieldName' => 'contributorCount',
                    'property' => 'dct:contributor',
                    'temporary' => true,
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'creatorCount',
                    'value' => [
                        'conditional' => [
                            'if' => ['$creatorCount'],
                            'then' => '$creatorCount',
                            'else' => 0,
                        ],
                    ],
                    'temporary' => true,
                ],
                [
                    'fieldName' => 'contributorCount',
                    'value' => [
                        'conditional' => [
                            'if' => ['$contributorCount'],
                            'then' => '$contributorCount',
                            'else' => 0,
                        ],
                    ],
                    'temporary' => true,
                ],
                [
                    'fieldName' => 'totalContributorCount',
                    'value' => [
                        'arithmetic' => ['$creatorCount', '+', '$contributorCount'],
                    ],
                ],
            ],
        ];

        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_creator_count');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_creator_count');

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'baseData:foo1234']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals(1, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'baseData:bar1234']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(3, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(['_id.type' => 't_creator_count', '_id.r' => 'http://talisaspire.com/works/4d101f63c10a6-2']);
        $this->assertNotNull($tableDoc);
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testNestArithmeticInConditionalIf(): void
    {
        $tableSpec = [
            '_id' => 't_conditional_with_nested_arithmetic',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'computed_fields' => [
                [
                    'fieldName' => 'foobar',
                    'value' => [
                        'conditional' => [
                            'if' => [
                                ['arithmetic' => [3, '+', 3]],
                                '>',
                                ['arithmetic' => [4, '+', 3]], // obviously this should never be true
                            ],
                            'then' => 'a',
                            'else' => 'b',
                        ],
                    ],
                ],
            ],
        ];
        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_with_nested_arithmetic');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_with_nested_arithmetic');
        $tableDoc = $collection->findOne(['_id.type' => 't_conditional_with_nested_arithmetic']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals('b', $tableDoc['value']['foobar']);
        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }

    public function testNestConditionalInArithmeticFunction(): void
    {
        $tableSpec = [
            '_id' => 't_arithmetic_with_nested_conditional',
            'type' => ['bibo:Book', 'bibo:Document'],
            'from' => 'CBD_testing',
            'fields' => [
                [
                    'fieldName' => 'x',
                    'predicates' => ['foo:wibble'],
                ],
            ],
            'computed_fields' => [
                [
                    'fieldName' => 'foobar',
                    'value' => [
                        'arithmetic' => [
                            [
                                'conditional' => [
                                    'if' => ['$x'], // Not set, so should be false
                                    'then' => '$x',
                                    'else' => 100,
                                ],
                            ],
                            '*',
                            3,
                        ],
                    ],
                ],
            ],
        ];
        $oldConfig = Config::getConfig();
        $newConfig = Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        Config::setConfig($newConfig);
        Config::getInstance();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_arithmetic_with_nested_conditional');
        $collection = Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_arithmetic_with_nested_conditional');
        $tableDoc = $collection->findOne(['_id.type' => 't_arithmetic_with_nested_conditional']);
        $this->assertNotNull($tableDoc);

        $this->assertEquals(300, $tableDoc['value']['foobar']);
        Config::setConfig($oldConfig);
        Config::getInstance();
        $collection->drop();
    }
}
