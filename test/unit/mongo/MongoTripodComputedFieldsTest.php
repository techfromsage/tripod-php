<?php

require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/delegates/Tables.class.php';
require_once 'src/mongo/Driver.class.php';
require_once 'src/mongo/MongoGraph.class.php';

class MongoTripodComputedFieldsTest extends MongoTripodTestBase
{
    protected $originalConfig = array();

    public function setUp()
    {
        parent::setUp();
        $this->originalConfig = \Tripod\Config::getConfig();
        \Tripod\Mongo\Config::setValidationLevel(\Tripod\Mongo\Config::VALIDATE_MAX);
    }

    public function tearDown()
    {
        \Tripod\Config::setConfig($this->originalConfig);
        \Tripod\Mongo\Config::setValidationLevel(\Tripod\Mongo\Config::VALIDATE_MIN);
        parent::tearDown();
    }

    public function testConditionalComputedFieldWithDates() {
        $tableSpec = array(
            "_id"=> "t_conditional_creators",
            "type"=> array("bibo:Document"),
            "from"=>"CBD_testing",
            "fields"=> array(
                array(
                    "fieldName" => "dateUpdated",
                    "predicates" => array(array(
                        "date" => array(
                            "predicates" => array("dct:updated")
                        )
                    ))
                ),
                array(
                    "fieldName" => "datePublished",
                    "predicates" => array(array(
                        "date" => array(
                            "predicates" => array("dct:published")
                        )
                    ))
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName" => "status",
                    "value" => array(
                        "conditional" => array(
                            "if" => array('$dateUpdated', '>', '$datePublished'),
                            "then" => 'Updated',
                            "else" => 'Published'
                        )
                    )
                )
            )
        );

        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadDatesDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');

        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r' => 'baseData:foo1234'));
        $this->assertEquals('Updated', $tableDoc['value']['status']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r' => 'baseData:foo12345'));
        $this->assertEquals('Published', $tableDoc['value']['status']);

        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    public function testConditionalComputedField()
    {
        $tableSpec = array(
            "_id"=> "t_conditional_creators",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "counts"=> array(
                array(
                    "fieldName"=>"creatorCount",
                    "property"=>"dct:creator"
                ),
                array(
                    "fieldName"=>"contributorCount",
                    "property"=>"dct:contributor"
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"creatorCount",
                    "value"=>array(
                        "conditional"=>array(
                            "if"=>array('$creatorCount'),
                            "then"=>'$creatorCount',
                            "else"=>1234
                        )
                    )
                ),
                array(
                    "fieldName"=> "contributorCount",
                    "value"=> array(
                        "conditional"=> array(
                            "if"=>array('$contributorCount'),
                            "then"=>'$contributorCount',
                            "else"=> 1234
                        )
                    )
                ),
            )
        );

        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(1234, $tableDoc['value']['contributorCount']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(2, $tableDoc['value']['contributorCount']);

        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    protected function generateUniqueTableId($prefix)
    {
        return uniqid($prefix);
    }
    public function testNestedConditionalComputedField()
    {

        $tableSpec = array(
            "_id"=> "t_conditional_creators",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "counts"=> array(
                array(
                    "fieldName"=>"creatorCount",
                    "property"=>"dct:creator",
                    "temporary"=>true
                ),
                array(
                    "fieldName"=>"contributorCount",
                    "property"=>"dct:contributor",
                    "temporary"=>true
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"normalizedCreatorCount",
                    "value"=>array(
                        "conditional"=>array(
                            "if"=>array('$contributorCount'),
                            "then"=>'$contributorCount',
                            "else"=>array(
                                "conditional"=> array(
                                    "if"=>array('$creatorCount'),
                                    "then"=>'$creatorCount',
                                    "else"=> "NO CONTRIBUTORS FOUND"
                                )
                            )
                        )
                    )
                )
            )
        );

        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'] = array($tableSpec);
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals(1, $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals(2, $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertEquals("NO CONTRIBUTORS FOUND", $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2'));
        $this->assertEquals("NO CONTRIBUTORS FOUND", $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals("NO CONTRIBUTORS FOUND", $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6-2'));
        $this->assertEquals("NO CONTRIBUTORS FOUND", $tableDoc['value']['normalizedCreatorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    public function testReplaceComputedField()
    {
        $tableSpec = array(
            "_id"=> "t_replace_type",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "fields"=> array(
                array(
                    "fieldName"=>"rdfType",
                    "predicates"=>array(array(
                        "join"=>array("glue"=>" ", "predicates"=>array("rdf:type"))
                    )),
                    "temporary"=>true
                ),
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"resourceType",
                    "value"=>array(
                        "replace"=>array(
                            "search"=>"bibo:",
                            "replace"=>"",
                            "subject"=>'$rdfType'
                        )
                    )
                ),
                array(
                    "fieldName"=>"resourceType",
                    "value"=>array(
                        "replace"=>array(
                            "search"=>"acorn:",
                            "replace"=>"",
                            "subject"=>'$resourceType'
                        )
                    )
                )
            )
        );

        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_replace_type');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_replace_type');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertEquals('Book Resource', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);


        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2'));
        $this->assertEquals('Book Resource', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);


        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6-2'));
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('rdfType', $tableDoc['value']);

        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    public function testArithmeticComputedField()
    {
        $tableSpec = array(
            "_id"=> "t_creator_count",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "counts"=> array(
                array(
                    "fieldName"=>"creatorCount",
                    "property"=>"dct:creator",
                    "temporary"=>true
                ),
                array(
                    "fieldName"=>"contributorCount",
                    "property"=>"dct:contributor",
                    "temporary"=>true
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"creatorCount",
                    "value"=>array(
                        "conditional"=>array(
                            "if"=>array('$creatorCount'),
                            "then"=>'$creatorCount',
                            "else"=>0
                        )
                    ),
                    "temporary"=>true
                ),
                array(
                    "fieldName"=> "contributorCount",
                    "value"=> array(
                        "conditional"=> array(
                            "if"=>array('$contributorCount'),
                            "then"=>'$contributorCount',
                            "else"=> 0
                        )
                    ),
                    "temporary"=>true
                ),
                array(
                    "fieldName"=>"totalContributorCount",
                    "value"=>array(
                        "arithmetic"=>array('$creatorCount',"+",'$contributorCount')
                    )
                )
            )
        );

        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_creator_count');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_creator_count');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals(1, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals(3, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6-2'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('creatorCount', $tableDoc['value']);

        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    public function testNestArithmeticInConditionalIf()
    {
        $tableSpec = array(
            "_id"=> "t_conditional_with_nested_arithmetic",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "computed_fields"=>array(
                array(
                    "fieldName"=>"foobar",
                    "value"=>array(
                        'conditional'=>array(
                            'if'=>array(
                                array('arithmetic'=>array(3,"+",3)),
                                ">",
                                array('arithmetic'=>array(4,"+",3)) // obviously this should never be true
                            ),
                            'then'=>'a',
                            'else'=>'b'
                        )
                    )
                )
            )
        );
        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_with_nested_arithmetic');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_with_nested_arithmetic');
        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_with_nested_arithmetic'));

        $this->assertEquals('b', $tableDoc['value']['foobar']);
        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }

    public function testNestConditionalInArithmeticFunction()
    {
        $tableSpec = array(
            "_id"=> "t_arithmetic_with_nested_conditional",
            "type"=> array("bibo:Book", "bibo:Document"),
            "from"=>"CBD_testing",
            "fields"=>array(
                array(
                    "fieldName"=>"x",
                    "predicates"=>array("foo:wibble")
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"foobar",
                    "value"=>array(
                        "arithmetic"=>array(
                            array(
                                "conditional"=>array(
                                    "if"=>array('$x'), // Not set, so should be false
                                    "then"=>'$x',
                                    "else"=>100
                                )
                            ),
                            "*",
                            3
                        )
                    )
                )
            )
        );
        $oldConfig = \Tripod\Config::getConfig();
        $newConfig = \Tripod\Config::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        \Tripod\Config::setConfig($newConfig);
        \Tripod\Config::getInstance();
        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing');
        $this->loadResourceDataViaTripod();
        $this->tripod->generateTableRows('t_arithmetic_with_nested_conditional');
        $collection = \Tripod\Config::getInstance()->getCollectionForTable('tripod_php_testing', 't_arithmetic_with_nested_conditional');
        $tableDoc = $collection->findOne(array('_id.type'=>'t_arithmetic_with_nested_conditional'));

        $this->assertEquals(300, $tableDoc['value']['foobar']);
        \Tripod\Config::setConfig($oldConfig);
        \Tripod\Config::getInstance();
        $collection->drop();
    }
}