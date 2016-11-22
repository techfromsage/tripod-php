<?php

use Tripod\ITripodStat;
use \MongoDB\BSON\UTCDateTime;
use \MongoDB\Collection;

set_include_path(
  get_include_path()
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__))))
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/lib'
  . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/src');

require_once('tripod.inc.php');
require_once TRIPOD_DIR . 'mongo/Config.class.php';
require_once TRIPOD_DIR . 'mongo/base/DriverBase.class.php';

/**
 * Mongo Config For Main DB
 */
define('MONGO_MAIN_DB', 'acorn');
define('MONGO_MAIN_COLLECTION', 'CBD_harvest');
define('MONGO_USER_COLLECTION', 'CBD_user');

/**
 * Class MongoTripodTestBase
 */
abstract class MongoTripodTestBase extends PHPUnit_Framework_TestCase
{

    /**
     * @var \Tripod\Mongo\Driver
     */
    protected $tripod = null;
    /**
     * @var \Tripod\Mongo\TransactionLog
     */
    protected $tripodTransactionLog = null;

    protected function tearDown()
    {
        // these are important to keep the Mongo open connection pool size down!
        $this->tripod = null;
        $this->tripodTransactionLog = null;
    }


    protected function loadResourceData()
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__).'/data/resources.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }

    protected function loadResourceDataViaTripod()
    {
        $this->loadDataViaTripod('/data/resources.json');
    }

    protected function loadBaseSearchDataViaTripod()
    {
        $this->loadDataViaTripod('/data/searchData.json');
    }

    /**
     * @param string $filename
     */
    private function loadDataViaTripod($filename){
        $docs = json_decode(file_get_contents(dirname(__FILE__).$filename), true);
        foreach ($docs as $d)
        {
            $g = new \Tripod\Mongo\MongoGraph();
            $g->add_tripod_array($d);
            $this->tripod->saveChanges(new \Tripod\ExtendedGraph(), $g,$d['_id'][_ID_CONTEXT]);
        }
    }

    protected function getConfigLocation()
    {
        return dirname(__FILE__).'/data/config.json';
    }

    protected function setUp()
    {
        date_default_timezone_set('Europe/London');

        $config = json_decode(file_get_contents($this->getConfigLocation()), true);
        if(getenv('TRIPOD_DATASOURCE_RS1_CONFIG'))
        {
            $config['data_sources']['rs1'] = json_decode(getenv('TRIPOD_DATASOURCE_RS1_CONFIG'), true);
        }
        if(getenv('TRIPOD_DATASOURCE_RS2_CONFIG'))
        {
            $config['data_sources']['rs2'] = json_decode(getenv('TRIPOD_DATASOURCE_RS2_CONFIG'), true);
        }
        \Tripod\Mongo\Config::setConfig($config);

        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";

        // make sure log statements don't go to stdout during tests...
        $log = new \Monolog\Logger("unittest");
        $log->pushHandler(new \Monolog\Handler\NullHandler());
        \Tripod\Mongo\DriverBase::$logger = $log;
    }


    /****************** HELPERS BELOW HERE ********************************/

    protected function addDocument($doc, $toTransactionLog=false)
    {
        $config = \Tripod\Mongo\Config::getInstance();
        if($toTransactionLog == true)
        {
            return $this->getTlogCollection()->insertOne($doc, array("w"=>1));
        } else {
            return $config->getCollectionForCBD(
                $this->tripod->getStoreName(),
                $this->tripod->getPodName()
            )->insertOne($doc, array("w"=>1));
        }
    }

    /**
     * @return Collection
     */
    protected function getTlogCollection()
    {
        $config = \Tripod\Mongo\Config::getInstance();
        $tLogConfig = $config->getTransactionLogConfig();
        return $config->getTransactionLogDatabase()->selectCollection($tLogConfig['collection']);
    }

    /**
     * @param \Tripod\Mongo\Driver $tripod
     * @return Collection
     */
    protected function getTripodCollection(\Tripod\Mongo\Driver $tripod)
    {
        $config = \Tripod\Mongo\Config::getInstance();
        $podName = $tripod->getPodName();
        $dataSource = $config->getDataSourceForPod($tripod->getStoreName(), $podName);
        return $config->getDatabase(
            $tripod->getStoreName(),
            $dataSource
        )->selectCollection($tripod->getPodName());
    }

    /**
     * @param mixed $_id
     * @param Collection|null $collection
     * @param bool $fromTransactionLog
     * @return array|null
     */
    protected function getDocument($_id, $collection=null, $fromTransactionLog=false)
    {
        if($fromTransactionLog==true)
        {
            return $this->tripodTransactionLog->getTransaction($_id);
        }

        if($collection==NULL)
        {
            return $this->getTripodCollection($this->tripod)->findOne(array("_id"=>$_id));
        }
        elseif($collection instanceof \Tripod\Mongo\Driver)
        {
            return $this->getTripodCollection($collection)->findOne(array("_id"=>$_id));
        }
        else
        {
            return $collection->findOne(array("_id"=>$_id));
        }
    }

    /**
     * @param array $changes
     * @param string $subjectOfChange
     * @param int $expectedNumberOfAdditions
     * @param int $expectedNumberOfRemovals
     */
    protected function assertChangesForGivenSubject(Array $changes, $subjectOfChange, $expectedNumberOfAdditions, $expectedNumberOfRemovals)
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

    /**
     * @param array $doc
     * @param string $key
     */
    protected function assertTransactionDate(Array $doc, $key)
    {
        $this->assertTrue(isset($doc[$key]), 'the date property: {$key} was not present in document');
        $this->assertInstanceOf('MongoDB\BSON\UTCDateTime', $doc[$key]);
        $this->assertNotEmpty($doc[$key]->toDateTime());
    }

    /**
     * @param mixed $_id
     * @param int|null $expectedValue
     * @param bool $hasVersion
     * @param \Tripod\Mongo\Driver|null $tripod
     * @param bool $fromTransactionLog
     */
    protected function assertDocumentVersion($_id, $expectedValue=null, $hasVersion=true, $tripod=null, $fromTransactionLog=false)
    {
        // just make sure $_id is aliased
        $labeller = new \Tripod\Mongo\Labeller();
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
        $labeller = new \Tripod\Mongo\Labeller();
        foreach ($_id as $key=>$value)
        {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertTrue(isset($doc[$property]), "Document for ".var_export($_id,true)." should have property [$property], but none found");
        if($expectedValue !== NULL){
            $this->assertEquals($expectedValue, $doc[$property],  "Document property [$property] actual value [".print_r($doc[$property], true)."] does not match expected value [" . print_r($expectedValue, true) . "]");
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
        $labeller = new \Tripod\Mongo\Labeller();
        foreach ($_id as $key=>$value)
        {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertFalse(isset($doc[$property]), "Document for ".var_export($_id,true)." should not have property [$property], but propert was found");
    }


    /**
     * @param mixed $_id
     * @param \Tripod\Mongo\Driver|null $tripod
     * @param bool $fromTransactionLog
     */
    protected function assertDocumentExists($_id, $tripod=null, $fromTransactionLog=false)
    {
        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        $this->assertNotNull($doc);
        $this->assertEquals($_id, $doc["_id"], "Actual Document _id :[" . print_r($doc['_id'], true) . "] did not match expected value of " . print_r($_id, true));
    }

    /**
     * @param mixed $_id
     * @param \Tripod\Mongo\Driver|null $tripod
     * @param bool $useTransactionTripod
     */
    protected function assertDocumentHasBeenDeleted($_id, $tripod=null, $useTransactionTripod=false)
    {
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

    /**
     * @param \Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertHasLiteralTriple(\Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_literal_triple($s, $p, $o), "Graph did not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    /**
     * @param \Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertHasResourceTriple(\Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_resource_triple($s, $p, $o), "Graph did not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    /**
     * @param \Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertDoesNotHaveLiteralTriple(\Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_literal_triple($s, $p, $o), "Graph should not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    /**
     * @param \Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertDoesNotHaveResourceTriple(\Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_resource_triple($s, $p, $o), "Graph should not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    /**
     * @param string $subject
     * @param string $transaction_id
     */
    protected function lockDocument($subject, $transaction_id)
    {
        $collection = \Tripod\Mongo\Config::getInstance()->getCollectionForLocks('tripod_php_testing');
        $labeller = new \Tripod\Mongo\Labeller();
        $doc = array(
            '_id' => array(_ID_RESOURCE => $labeller->uri_to_alias($subject), _ID_CONTEXT => \Tripod\Mongo\Config::getInstance()->getDefaultContextAlias()),
            _LOCKED_FOR_TRANS => $transaction_id,
            _LOCKED_FOR_TRANS_TS => new UTCDateTime(floor(microtime(true)*1000))
        );
        $collection->insertOne($doc, array("w" => 1));
    }

    /**
     * @param string $host
     * @param string|int $port
     * @param string $prefix
     * @return PHPUnit_Framework_MockObject_MockObject|\Tripod\StatsD
     */
    protected function getMockStat($host, $port, $prefix='', array $mockedMethods = array())
    {
        $mockedMethods = array_merge(array('send'), $mockedMethods);
        /** @var \Tripod\StatsD|PHPUnit_Framework_MockObject_MockObject $stat */
        $stat = $this->getMockBuilder('\Tripod\StatsD')
            ->setMethods($mockedMethods)
            ->setConstructorArgs(array($host, $port, $prefix))
            ->getMock();

        return $stat;
    }

    /**
     * @return array
     */
    protected function getStatsDConfig()
    {
        return array(
            'class'=>'Tripod\StatsD',
            'config'=>array(
                'host'=>'example.com',
                'port'=>1234,
                'prefix'=>'somePrefix'
            )
        );
    }
}

/**
 * Class TestTripod
 */
class TestTripod extends \Tripod\Mongo\Driver
{
    /**
     * @return array
     */
    public function getCollectionReadPreference()
    {
        return $this->collection->__debugInfo()['readPreference'];
    }
}

/**
 * Class TripodTestConfig
 */
class TripodTestConfig extends \Tripod\Mongo\Config
{
    /**
     * Constructor
     */
    public function __construct(){}

    /**
     * @param array $config
     */
    public function loadConfig(array $config)
    {
        parent::loadConfig($config);
    }
}