<?php

use MongoDB\Collection;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

abstract class MongoTripodTestBase extends TestCase
{
    /**
     * @var Tripod\Mongo\Driver
     */
    protected $tripod;

    /**
     * @var Tripod\Mongo\TransactionLog
     */
    protected $tripodTransactionLog;

    protected function tearDown(): void
    {
        // these are important to keep the Mongo open connection pool size down!
        $this->tripod = null;
        $this->tripodTransactionLog = null;
    }

    protected function loadResourceData()
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__) . '/data/resources.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }

    protected function loadDatesDataViaTripod()
    {
        $this->loadDataViaTripod($this->tripod, '/data/dates.json');
    }

    protected function loadResourceDataViaTripod()
    {
        $this->loadDataViaTripod($this->tripod,'/data/resources.json');
    }

    protected function loadBaseSearchDataViaTripod()
    {
        $this->loadDataViaTripod($this->tripod,'/data/searchData.json');
    }

    protected function loadRelatedContentIntoTripod()
    {
        $relatedContentTripod = new Tripod\Mongo\Driver(
            'CBD_test_related_content',
            'tripod_php_testing',
            [
                'defaultContext' => 'http://talisaspire.com/',
                'async' => [OP_VIEWS => true], // don't generate views syncronously when saving automatically - let unit tests deal with this)
            ],
        );

        $this->loadDataViaTripod($relatedContentTripod,'/data/relatedContent.json');
    }

    /**
     * @param string $filename
     */
    private function loadDataViaTripod(Tripod\Mongo\Driver $tripod, $filename)
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__) . $filename), true);
        foreach ($docs as $d) {
            $g = new Tripod\Mongo\MongoGraph();
            $g->add_tripod_array($d);
            $tripod->saveChanges(new Tripod\ExtendedGraph(), $g, $d['_id'][_ID_CONTEXT]);
        }
    }

    protected function getConfigLocation()
    {
        return dirname(__FILE__) . '/data/config.json';
    }

    protected function setUp(): void
    {
        date_default_timezone_set('UTC');

        $config = json_decode(file_get_contents($this->getConfigLocation()), true);
        if (getenv('TRIPOD_DATASOURCE_RS1_CONFIG')) {
            $config['data_sources']['rs1'] = json_decode(getenv('TRIPOD_DATASOURCE_RS1_CONFIG'), true);
        }
        if (getenv('TRIPOD_DATASOURCE_RS2_CONFIG')) {
            $config['data_sources']['rs2'] = json_decode(getenv('TRIPOD_DATASOURCE_RS2_CONFIG'), true);
        }
        Tripod\Config::setConfig($config);

        printf(" %s->%s\n", get_class($this), $this->getName());
    }

    // HELPERS BELOW HERE

    protected function addDocument($doc, $toTransactionLog = false)
    {
        $config = Tripod\Config::getInstance();
        if ($toTransactionLog == true) {
            return $this->getTlogCollection()->insertOne($doc, ['w' => 1]);
        }  
            return $config->getCollectionForCBD(
                $this->tripod->getStoreName(),
                $this->tripod->getPodName()
            )->insertOne($doc, ['w' => 1]);
        
    }

    /**
     * @return Collection
     */
    protected function getTlogCollection()
    {
        $config = Tripod\Config::getInstance();
        $tLogConfig = $config->getTransactionLogConfig();
        return $config->getTransactionLogDatabase()->selectCollection($tLogConfig['collection']);
    }

    /**
     * @param Tripod\Mongo\Driver $tripod
     * @return Collection
     */
    protected function getTripodCollection(Tripod\Mongo\Driver $tripod)
    {
        $config = Tripod\Config::getInstance();
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
    protected function getDocument($_id, $collection = null, $fromTransactionLog = false)
    {
        if ($fromTransactionLog == true) {
            return $this->tripodTransactionLog->getTransaction($_id);
        }

        if ($collection == null) {
            return $this->getTripodCollection($this->tripod)->findOne(['_id' => $_id]);
        }
        if ($collection instanceof Tripod\Mongo\Driver) {
            return $this->getTripodCollection($collection)->findOne(['_id' => $_id]);
        }  
            return $collection->findOne(['_id' => $_id]);
        
    }

    /**
     * @param array $changes
     * @param string $subjectOfChange
     * @param int $expectedNumberOfAdditions
     * @param int $expectedNumberOfRemovals
     */
    protected function assertChangesForGivenSubject(array $changes, $subjectOfChange, $expectedNumberOfAdditions, $expectedNumberOfRemovals)
    {
        $changeSet = null;

        foreach ($changes as $c) {
            if (strpos($c['_id']['r'], '_:cs') !== false) {
                if ($c['cs:subjectOfChange']['u'] == $subjectOfChange) {
                    $changeSet = $c;
                }
            }
        }

        $this->assertNotNull($changeSet, 'No change set found for the specified subject of change');

        $actualAdditions = 0;
        if (isset($changeSet['cs:addition'])) {
            if (isset($changeSet['cs:addition']['u'])) {
                $actualAdditions = 1; // mongo tripod document optimisation for one value...
            } else {
                $actualAdditions = count($changeSet['cs:addition']);
            }
        }
        $this->assertEquals($expectedNumberOfAdditions, $actualAdditions, 'Number of additions did not match expectd value');

        $actualRemovals = 0;
        if (isset($changeSet['cs:removal'])) {
            if (isset($changeSet['cs:removal']['value'])) {
                $actualRemovals = 1; // mongo tripod document optimisation for one value...
            } else {
                $actualRemovals = count($changeSet['cs:removal']);
            }
        }

        $this->assertEquals($expectedNumberOfRemovals, $actualRemovals, 'Number of removals did not match expectd value');
    }

    /**
     * @param array $doc
     * @param string $key
     */
    protected function assertTransactionDate(array $doc, $key)
    {
        $this->assertTrue(isset($doc[$key]), 'the date property: {$key} was not present in document');
        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $doc[$key]);
        $this->assertNotEmpty($doc[$key]->toDateTime());
    }

    /**
     * @param mixed $_id
     * @param int|null $expectedValue
     * @param bool $hasVersion
     * @param Tripod\Mongo\Driver|null $tripod
     * @param bool $fromTransactionLog
     */
    protected function assertDocumentVersion($_id, $expectedValue = null, $hasVersion = true, $tripod = null, $fromTransactionLog = false)
    {
        // just make sure $_id is aliased
        $labeller = new Tripod\Mongo\Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        if ($hasVersion == true) {
            $this->assertTrue(isset($doc['_version']), 'Document for ' . var_export($_id, true) . ' should have a version, but none found');

            if ($expectedValue !== null) {
                // echo $expectedValue.":".$doc['_version'];
                $this->assertEquals($expectedValue, $doc['_version'], 'Document version does not match expected version');
            }
        } else {
            $this->assertFalse(isset($doc['_version']), 'Was not expecting document to have a version');
        }
    }

    /**
     * @param $_id = the id of the document to retrieve from mongo
     * @param $property = the property you are checking for
     * @param null $expectedValue = if not null the property value will be matched against this expectedValue
     * @param null $tripod = optional tripod object, defaults to this->tripod
     * @param bool $fromTransactionLog = true if you want to retrieve the document from transaction log
     */
    protected function assertDocumentHasProperty($_id, $property, $expectedValue = null, $tripod = null, $fromTransactionLog = false)
    {
        // just make sure $_id is aliased
        $labeller = new Tripod\Mongo\Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertTrue(isset($doc[$property]), 'Document for ' . var_export($_id, true) . " should have property [{$property}], but none found");
        if ($expectedValue !== null) {
            $this->assertEquals($expectedValue, $doc[$property], "Document property [{$property}] actual value [" . print_r($doc[$property], true) . '] does not match expected value [' . print_r($expectedValue, true) . ']');
        }
    }

    /**
     * @param $_id = the id of the document to retrieve from mongo
     * @param $property = the property you want to make sure does not exist
     * @param null $tripod = optional tripod object, defaults to this->tripod
     * @param bool $fromTransactionLog = true if you want to retrieve the document from transaction log
     */
    protected function assertDocumentDoesNotHaveProperty($_id, $property, $tripod = null, $fromTransactionLog = false)
    {
        // just make sure $_id is aliased
        $labeller = new Tripod\Mongo\Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);

        $this->assertFalse(isset($doc[$property]), 'Document for ' . var_export($_id, true) . " should not have property [{$property}], but propert was found");
    }

    /**
     * @param mixed $_id
     * @param Tripod\Mongo\Driver|null $tripod
     * @param bool $fromTransactionLog
     */
    protected function assertDocumentExists($_id, $tripod = null, $fromTransactionLog = false)
    {
        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        $this->assertNotNull($doc);
        $this->assertEquals($_id, $doc['_id'], 'Actual Document _id :[' . print_r($doc['_id'], true) . '] did not match expected value of ' . print_r($_id, true));
    }

    /**
     * @param mixed $_id
     * @param Tripod\Mongo\Driver|null $tripod
     * @param bool $useTransactionTripod
     */
    protected function assertDocumentHasBeenDeleted($_id, $tripod = null, $useTransactionTripod = false)
    {
        $doc = $this->getDocument($_id, $tripod, $useTransactionTripod);
        if ($useTransactionTripod) {
            $this->assertNull($doc, "Document with _id:[{$_id}] exists, but it should not");
        } else {
            $this->assertTrue(is_array($doc), 'Document should be array');
            $keys = array_keys($doc);
            $this->assertEquals(4, count($keys));
            $this->assertArrayHasKey('_id', $doc);
            $this->assertArrayHasKey(_VERSION, $doc);
            $this->assertArrayHasKey(_CREATED_TS, $doc);
            $this->assertArrayHasKey(_UPDATED_TS, $doc);
        }
    }

    /**
     * @param Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertHasLiteralTriple(Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_literal_triple($s, $p, $o), "Graph did not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    /**
     * @param Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertHasResourceTriple(Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertTrue($graph->has_resource_triple($s, $p, $o), "Graph did not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    /**
     * @param Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertDoesNotHaveLiteralTriple(Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_literal_triple($s, $p, $o), "Graph should not contain the literal triple: <{$s}> <{$p}> \"{$o}\"");
    }

    /**
     * @param Tripod\ExtendedGraph $graph
     * @param string $s
     * @param string $p
     * @param string $o
     */
    protected function assertDoesNotHaveResourceTriple(Tripod\ExtendedGraph $graph, $s, $p, $o)
    {
        $this->assertFalse($graph->has_resource_triple($s, $p, $o), "Graph should not contain the resource triple: <{$s}> <{$p}> <{$o}>");
    }

    /**
     * @param string $subject
     * @param string $transaction_id
     */
    protected function lockDocument($subject, $transaction_id)
    {
        $collection = Tripod\Config::getInstance()->getCollectionForLocks('tripod_php_testing');
        $labeller = new Tripod\Mongo\Labeller();
        $doc = [
            '_id' => [_ID_RESOURCE => $labeller->uri_to_alias($subject), _ID_CONTEXT => Tripod\Config::getInstance()->getDefaultContextAlias()],
            _LOCKED_FOR_TRANS => $transaction_id,
            _LOCKED_FOR_TRANS_TS => Tripod\Mongo\DateUtil::getMongoDate(),
        ];
        $collection->insertOne($doc, ['w' => 1]);
    }

    /**
     * @param string $host
     * @param string|int $port
     * @param string $prefix
     * @return MockObject&\Tripod\StatsD
     */
    protected function getMockStat($host, $port, $prefix = '', array $mockedMethods = [])
    {
        $mockedMethods = array_merge(['send'], $mockedMethods);
        return $this->getMockBuilder('\Tripod\StatsD')
            ->onlyMethods($mockedMethods)
            ->setConstructorArgs([$host, $port, $prefix])
            ->getMock();
    }

    /**
     * @return array
     */
    protected function getStatsDConfig()
    {
        return [
            'class' => 'Tripod\StatsD',
            'config' => [
                'host' => 'example.com',
                'port' => 1234,
                'prefix' => 'somePrefix',
            ],
        ];
    }
}

class TestTripod extends Tripod\Mongo\Driver
{
    /**
     * @return MongoDB\Driver\ReadPreference
     */
    public function getCollectionReadPreference()
    {
        return $this->collection->__debugInfo()['readPreference'];
    }
}

class TripodTestConfig extends Tripod\Mongo\Config
{
    /**
     * Constructor
     */
    public function __construct() {}

    /**
     * @param array $config
     */
    public function loadConfig(array $config)
    {
        parent::loadConfig($config);
    }
}
