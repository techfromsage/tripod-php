<?php

require_once 'MongoTripodTestBase.php';
require_once 'src/mongo/delegates/MongoTripodTables.class.php';
require_once 'src/mongo/MongoTripod.class.php';
require_once 'src/mongo/MongoGraph.class.php';

class MongoTripodComputedFieldsTest extends MongoTripodTestBase
{
    protected $originalConfig = array();

    public function setUp()
    {
        parent::setUp();
        $this->originalConfig = MongoTripodConfig::getConfig();
        MongoTripodConfig::setValidationLevel(MongoTripodConfig::VALIDATE_MAX);
    }

    public function tearDown()
    {
        MongoTripodConfig::setConfig($this->originalConfig);
        MongoTripodConfig::setValidationLevel(MongoTripodConfig::VALIDATE_MIN);
        parent::tearDown();
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
                        "_conditional_"=>array(
                            "if"=>array('$creatorCount'),
                            "then"=>'$creatorCount',
                            "else"=>1234
                        )
                    )
                ),
                array(
                    "fieldName"=> "contributorCount",
                    "value"=> array(
                        "_conditional_"=> array(
                            "if"=>array('$contributorCount'),
                            "then"=>'$contributorCount',
                            "else"=> 1234
                        )
                    )
                ),
            )
        );

        $oldConfig = MongoTripodConfig::getConfig();
        $newConfig = MongoTripodConfig::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        MongoTripodConfig::setConfig($newConfig);
        MongoTripodConfig::getInstance();
        $this->tripod = new MongoTripod('CBD_testing', 'tripod_php_testing');
        $this->loadBaseDataViaTripod();
        $this->tripod->generateTableRows('t_conditional_creators');
        $collection = MongoTripodConfig::getInstance()->getCollectionForTable('tripod_php_testing', 't_conditional_creators');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(1234, $tableDoc['value']['contributorCount']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_conditional_creators', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals(1, $tableDoc['value']['creatorCount']);
        $this->assertEquals(2, $tableDoc['value']['contributorCount']);

        MongoTripodConfig::setConfig($oldConfig);
        MongoTripodConfig::getInstance();
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
                    "fieldName"=>"!rdfType",
                    "predicates"=>array(array(
                        "join"=>array("glue"=>" ", "predicates"=>array("rdf:type"))
                    ))
                ),
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"resourceType",
                    "value"=>array(
                        "_replace_"=>array(
                            "search"=>"bibo:",
                            "replace"=>"",
                            "subject"=>'$!rdfType'
                        )
                    )
                ),
                array(
                    "fieldName"=>"resourceType",
                    "value"=>array(
                        "_replace_"=>array(
                            "search"=>"acorn:",
                            "replace"=>"",
                            "subject"=>'$resourceType'
                        )
                    )
                )
            )
        );

        $oldConfig = MongoTripodConfig::getConfig();
        $newConfig = MongoTripodConfig::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        MongoTripodConfig::setConfig($newConfig);
        MongoTripodConfig::getInstance();
        $this->tripod = new MongoTripod('CBD_testing', 'tripod_php_testing');
        $this->loadBaseDataViaTripod();
        $this->tripod->generateTableRows('t_replace_type');
        $collection = MongoTripodConfig::getInstance()->getCollectionForTable('tripod_php_testing', 't_replace_type');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals('Document', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertEquals('Book Resource', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);


        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2'));
        $this->assertEquals('Book Resource', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);


        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_replace_type', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6-2'));
        $this->assertEquals('Book Work', $tableDoc['value']['resourceType']);
        $this->assertArrayNotHasKey('!rdfType', $tableDoc['value']);

        MongoTripodConfig::setConfig($oldConfig);
        MongoTripodConfig::getInstance();
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
                    "fieldName"=>"!creatorCount",
                    "property"=>"dct:creator"
                ),
                array(
                    "fieldName"=>"!contributorCount",
                    "property"=>"dct:contributor"
                )
            ),
            "computed_fields"=>array(
                array(
                    "fieldName"=>"!creatorCount",
                    "value"=>array(
                        "_conditional_"=>array(
                            "if"=>array('$!creatorCount'),
                            "then"=>'$!creatorCount',
                            "else"=>0
                        )
                    )
                ),
                array(
                    "fieldName"=> "!contributorCount",
                    "value"=> array(
                        "_conditional_"=> array(
                            "if"=>array('$!contributorCount'),
                            "then"=>'$!contributorCount',
                            "else"=> 0
                        )
                    )
                ),
                array(
                    "fieldName"=>"totalContributorCount",
                    "value"=>array(
                        "_arithmetic_"=>array("$!creatorCount","+","$!contributorCount")
                    )
                )
            )
        );

        $oldConfig = MongoTripodConfig::getConfig();
        $newConfig = MongoTripodConfig::getConfig();
        $newConfig['stores']['tripod_php_testing']['table_specifications'][] = $tableSpec;
        MongoTripodConfig::setConfig($newConfig);
        MongoTripodConfig::getInstance();
        $this->tripod = new MongoTripod('CBD_testing', 'tripod_php_testing');
        $this->loadBaseDataViaTripod();
        $this->tripod->generateTableRows('t_creator_count');
        $collection = MongoTripodConfig::getInstance()->getCollectionForTable('tripod_php_testing', 't_creator_count');

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'baseData:foo1234'));

        $this->assertEquals(1, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'baseData:bar1234'));
        $this->assertEquals(3, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/resources/3SplCtWGPqEyXcDiyhHQpA-2'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        $tableDoc = $collection->findOne(array('_id.type'=>'t_creator_count', '_id.r'=>'http://talisaspire.com/works/4d101f63c10a6-2'));
        $this->assertEquals(0, $tableDoc['value']['totalContributorCount']);
        $this->assertArrayNotHasKey('!contributorCount', $tableDoc['value']);
        $this->assertArrayNotHasKey('!creatorCount', $tableDoc['value']);

        MongoTripodConfig::setConfig($oldConfig);
        MongoTripodConfig::getInstance();
        $collection->drop();
    }
}