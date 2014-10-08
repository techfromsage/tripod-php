<?php

set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__))))
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/lib'
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/src');

require_once('tripod.inc.php');
require_once TRIPOD_DIR.'mongo/MongoTripodConfig.class.php';
require_once TRIPOD_DIR.'mongo/base/MongoTripodBase.class.php';

/**
 * Mongo Config For Main DB
 */
define('MONGO_MAIN_DB', 'acorn');
define('MONGO_MAIN_COLLECTION', 'CBD_harvest');
define('MONGO_USER_COLLECTION', 'CBD_user');

class AnonymousLogger
{
    static function getInstance()
    {
        return new AnonymousLogger();
    }
    function error($message,$params=null)
    {
        // useful, do something
        echo "$message\n";
        if ($params)
        {
            foreach ($params as $key=>$value)
            {
                echo "$key: " . print_r($value, true) . "\n";
            }
        }
    }
    function debug($message)
    {
        // do nothing
    }
}

class MongoTripodTestBase extends PHPUnit_Framework_TestCase
{

    /**
     * @var MongoTripod
     */
    protected $tripod = null;
    /**
     * @var MongoTransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function tearDown()
    {
        // these are important to keep the Mongo open connection pool size down!
        $this->tripod = null;
        $this->tripodTransactionLog = null;
    }


    protected function loadBaseData()
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__).'/data/resources.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }

    protected function loadBaseDataViaTripod()
    {
        $this->loadDataViaTripod('/data/resources.json');
    }

    protected function loadBaseSearchDataViaTripod()
    {
        $this->loadDataViaTripod('/data/searchData.json');
    }

    private function loadDataViaTripod($filename){
        $docs = json_decode(file_get_contents(dirname(__FILE__).$filename), true);
        foreach ($docs as $d)
        {
            $g = new MongoGraph();
            $g->add_tripod_array($d);
            $this->tripod->saveChanges(new ExtendedGraph(), $g,$d['_id'][_ID_CONTEXT]);
        }
    }

    protected function setUp()
    {
        date_default_timezone_set('Europe/London');
        $configFileName = dirname(__FILE__).'/data/config.json';

        $config = json_decode(file_get_contents($configFileName), true);
        MongoTripodConfig::setConfig($config);

        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";

        // make sure log statements don't go to stdout during tests...
        MongoTripodBase::$logger = new AnonymousLogger();
    }


    /****************** HELPERS BELOW HERE ********************************/

    protected function addDocument($doc, $toTransactionLog=false)
    {
        if($toTransactionLog == true){
            $tripod = new MongoTripod('transaction_log', 'testing');
            return $tripod->collection->insert($doc, array("w"=>1));
        } else {
            return $this->tripod->collection->insert($doc, array("w"=>1));
        }
    }

    protected function getDocument($_id, $tripod=null, $fromTransactionLog=false)
    {
        if($fromTransactionLog==true)
        {
            return $this->tripodTransactionLog->getTransaction($_id);
        }

        if($tripod==NULL)
        {
            return $this->tripod->collection->findOne(array("_id"=>$_id));
        }
        else
        {
            return $tripod->collection->findOne(array("_id"=>$_id));
        }
    }

    protected function assertChangesForGivenSubject($changes, $subjectOfChange, $expectedNumberOfAdditions, $expectedNumberOfRemovals)
    {
        $changeSet = null;

        foreach($changes as $c)
        {
            if(strpos($c['_id']["r"], '_:cs') !== FALSE)
            {
                if($c['cs:subjectOfChange']['u'] == $subjectOfChange)
                {
                    $changeSet = $c;
                }
            }
        }

        $this->assertNotNull($changeSet, "No change set found for the specified subject of change");

        $actualAdditions = 0;
        if(isset($changeSet['cs:addition']))
        {
            if (isset($changeSet['cs:addition']['u']))
            {
                $actualAdditions = 1; // mongo tripod document optimisation for one value...
            }
            else
            {
                $actualAdditions = count($changeSet['cs:addition']);
            }
        }
        $this->assertEquals($expectedNumberOfAdditions, $actualAdditions, "Number of additions did not match expectd value");

        $actualRemovals = 0;
        if(isset($changeSet['cs:removal']))
        {
            if (isset($changeSet['cs:removal']['value']))
            {
                $actualRemovals = 1; // mongo tripod document optimisation for one value...
            }
            else
            {
                $actualRemovals = count($changeSet['cs:removal']);
            }
        }

        $this->assertEquals($expectedNumberOfRemovals, $actualRemovals, "Number of removals did not match expectd value");
    }

    protected function assertTransactionDate($doc, $key)
    {
        $this->assertTrue(isset($doc[$key]), 'the date property: {$key} was not present in document');
        $this->assertTrue(!empty($doc[$key]->sec),'the date property: {$key} does not have a "sec" property');
        $this->assertTrue(!empty($doc[$key]->usec), 'the date property: {$key} does not have a "usec" property');
    }

    protected function assertDocumentVersion($_id, $expectedValue=null, $hasVersion=true, $tripod=null, $fromTransactionLog=false)
    {
        // just make sure $_id is aliased
        $labeller = new MongoTripodLabeller();
        foreach ($_id as $key=>$value)
        {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        if($hasVersion==true)
        {
            $this->assertTrue(isset($doc['_version']), "Document for ".var_export($_id,true)." should have a version, but none found");

            if($expectedValue!==NULL)
            {
                //echo $expectedValue.":".$doc['_version'];
                $this->assertEquals($expectedValue, $doc['_version'],  "Document version does not match expected version");
            }
        }
        else
        {
            $this->assertFalse(isset($doc['_version']), "Was not expecting document to have a version");
        }
    }

    /**
     * @param $_id = the id of the document to retrieve from mongo
     * @param $property = the property you are checking for
     * @param null $expectedValue = if not null the property value will be matched against this expectedValue
     * @param null $tripod = optional tripod object, defaults to this->tripod
     * @param bool $fromTransactionLog = true if you want to retrieve the document from transaction log
     */
    protected function assertDocumentHasProperty($_id, $property, $expectedValue=null, $tripod=null, $fromTransactionLog=false)
    {
        // just make sure $_id is aliased
        $labeller = new MongoTripodLabeller();
        foreach ($_id as $key=>$value)
        {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertTrue(isset($doc[$property]), "Document for ".var_export($_id,true)." should have property [$property], but none found");
        if($expectedValue !== NULL){
            $this->assertEquals($expectedValue, $doc[$property],  "Document property [$property] actual value [".$doc[$property]."] does not match expected value [$expectedValue]");
        }
    }

    /**
     * @param $_id = the id of the document to retrieve from mongo
     * @param $property = the property you want to make sure does not exist
     * @param null $tripod = optional tripod object, defaults to this->tripod
     * @param bool $fromTransactionLog = true if you want to retrieve the document from transaction log
     */
    protected function assertDocumentDoesNotHaveProperty($_id, $property, $tripod=null, $fromTransactionLog=false)
    {
        // just make sure $_id is aliased
        $labeller = new MongoTripodLabeller();
        foreach ($_id as $key=>$value)
        {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertFalse(isset($doc[$property]), "Document for ".var_export($_id,true)." should not have property [$property], but propert was found");
    }


    protected function assertDocumentExists($_id, $tripod=null, $fromTransactionLog=false)
    {
        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        $this->assertNotNull($doc);
        $this->assertEquals($_id, $doc["_id"], "Actual Document _id :[" . print_r($doc['_id']) . "] did not match expected value of " . print_r($_id));
    }

    protected function assertDocumentHasBeenDeleted($_id, $tripod=null, $useTransactionTripod=false)
    {
        //$this->assertNull($this->getDocument($_id, $useTransactionTripod), "Document with _id:[{$_id}] exists, but it should not");
        $doc = $this->getDocument($_id, $tripod, $useTransactionTripod);
        if($useTransactionTripod)
        {
            $this->assertNull($doc, "Document with _id:[{$_id}] exists, but it should not");
        }
        else
        {
            $this->assertTrue(is_array($doc),"Document should be array");
            $keys = array_keys($doc);
            $this->assertEquals(4, count($keys));
            $this->assertArrayHasKey('_id', $doc);
            $this->assertArrayHasKey(_VERSION, $doc);
            $this->assertArrayHasKey(_CREATED_TS, $doc);
            $this->assertArrayHasKey(_UPDATED_TS, $doc);
        }
    }

    protected function assertHasLiteralTriple(ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_literal_triple($s, $p, $o), "Graph did not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    protected function assertHasResourceTriple(ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_resource_triple($s, $p, $o), "Graph did not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    protected function assertDoesNotHaveLiteralTriple(ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_literal_triple($s, $p, $o), "Graph should not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    protected function assertDoesNotHaveResourceTriple(ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_resource_triple($s, $p, $o), "Graph should not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    protected function lockDocument($subject, $transaction_id)
    {
        $tripod = new MongoTripod(LOCKS_COLLECTION, 'testing');
        $labeller = new MongoTripodLabeller();
        $doc = array(
            '_id' => array(_ID_RESOURCE => $labeller->uri_to_alias($subject), _ID_CONTEXT => MongoTripodConfig::getInstance()->getDefaultContextAlias()),
            _LOCKED_FOR_TRANS => $transaction_id,
            _LOCKED_FOR_TRANS_TS=>new MongoDate()
        );
        $tripod->collection->insert($doc, array("w" => 1));
    }
}
