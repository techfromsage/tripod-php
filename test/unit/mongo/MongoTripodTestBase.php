<?php

declare(strict_types=1);

use MongoDB\BSON\UTCDateTime;
use MongoDB\Collection;
use MongoDB\Driver\ReadPreference;
use MongoDB\InsertOneResult;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Tripod\Config;
use Tripod\ExtendedGraph;
use Tripod\IDriver;
use Tripod\Mongo\DateUtil;
use Tripod\Mongo\Driver;
use Tripod\Mongo\Labeller;
use Tripod\Mongo\MongoGraph;
use Tripod\Mongo\TransactionLog;
use Tripod\StatsD;

abstract class MongoTripodTestBase extends TestCase
{
    protected Driver $tripod;

    protected TransactionLog $tripodTransactionLog;

    protected function setUp(): void
    {
        $config = json_decode((string) file_get_contents($this->getConfigLocation()), true);
        $rs1Config = getenv('TRIPOD_DATASOURCE_RS1_CONFIG');
        if ($rs1Config) {
            $config['data_sources']['rs1'] = json_decode($rs1Config, true);
        }

        $rs2Config = getenv('TRIPOD_DATASOURCE_RS2_CONFIG');
        if ($rs2Config) {
            $config['data_sources']['rs2'] = json_decode($rs2Config, true);
        }

        Config::setConfig($config);
    }

    protected function loadResourceData(): void
    {
        $docs = json_decode((string) file_get_contents(__DIR__ . '/data/resources.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }

    protected function loadDatesDataViaTripod(): void
    {
        $this->loadDataViaTripod($this->tripod, '/data/dates.json');
    }

    protected function loadResourceDataViaTripod(): void
    {
        $this->loadDataViaTripod($this->tripod, '/data/resources.json');
    }

    protected function loadBaseSearchDataViaTripod(): void
    {
        $this->loadDataViaTripod($this->tripod, '/data/searchData.json');
    }

    protected function loadRelatedContentIntoTripod(): void
    {
        $relatedContentTripod = new Driver(
            'CBD_test_related_content',
            'tripod_php_testing',
            [
                'defaultContext' => 'http://talisaspire.com/',
                'async' => [OP_VIEWS => true], // don't generate views syncronously when saving automatically - let unit tests deal with this)
            ],
        );

        $this->loadDataViaTripod($relatedContentTripod, '/data/relatedContent.json');
    }

    // HELPERS BELOW HERE

    protected function addDocument(array $doc, bool $toTransactionLog = false): InsertOneResult
    {
        $config = Config::getInstance();
        if ($toTransactionLog == true) {
            return $this->getTlogCollection()->insertOne($doc, ['w' => 1]);
        }

        return $config->getCollectionForCBD(
            $this->tripod->getStoreName(),
            $this->tripod->getPodName()
        )->insertOne($doc, ['w' => 1]);
    }

    protected function getTripodCollection(Driver $tripod): Collection
    {
        $config = Config::getInstance();
        $podName = $tripod->getPodName();
        $dataSource = $config->getDataSourceForPod($tripod->getStoreName(), $podName);

        return $config->getDatabase(
            $tripod->getStoreName(),
            $dataSource
        )->selectCollection($tripod->getPodName());
    }

    /**
     * @param array|string            $_id
     * @param Collection|IDriver|null $collection
     */
    protected function getDocument($_id, $collection = null, bool $fromTransactionLog = false): ?array
    {
        if ($fromTransactionLog) {
            if (!is_string($_id)) {
                throw new InvalidArgumentException('Transaction log lookups require a string id');
            }

            return $this->tripodTransactionLog->getTransaction($_id);
        }

        if ($collection == null) {
            return $this->getTripodCollection($this->tripod)->findOne(['_id' => $_id]);
        }

        if ($collection instanceof Driver) {
            return $this->getTripodCollection($collection)->findOne(['_id' => $_id]);
        }

        if (!$collection instanceof Collection) {
            throw new InvalidArgumentException('Expected a MongoDB collection or a Driver instance');
        }

        return $collection->findOne(['_id' => $_id]);
    }

    /**
     * @param string $subjectOfChange
     * @param int    $expectedNumberOfAdditions
     * @param int    $expectedNumberOfRemovals
     */
    protected function assertChangesForGivenSubject(array $changes, $subjectOfChange, $expectedNumberOfAdditions, $expectedNumberOfRemovals): void
    {
        $changeSet = null;

        foreach ($changes as $c) {
            if (strpos($c['_id']['r'], '_:cs') === false) {
                continue;
            }

            if ($c['cs:subjectOfChange']['u'] != $subjectOfChange) {
                continue;
            }

            $changeSet = $c;
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
     * @param array<string, mixed> $doc
     */
    protected function assertTransactionDate(array $doc, string $key): void
    {
        $this->assertArrayHasKey($key, $doc, 'the date property: {$key} was not present in document');
        $this->assertInstanceOf(UTCDateTime::class, $doc[$key]);
    }

    protected function assertDocumentVersion(array $_id, ?int $expectedValue = null, bool $hasVersion = true, ?Driver $tripod = null): void
    {
        // just make sure $_id is aliased
        $labeller = new Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod);
        $this->assertNotNull($doc);
        if ($hasVersion) {
            $this->assertArrayHasKey('_version', $doc, 'Document for ' . var_export($_id, true) . ' should have a version, but none found');

            if ($expectedValue !== null) {
                // echo $expectedValue.":".$doc['_version'];
                $this->assertEquals($expectedValue, $doc['_version'], 'Document version does not match expected version');
            }
        } else {
            $this->assertArrayNotHasKey('_version', $doc, 'Was not expecting document to have a version');
        }
    }

    /**
     * @param array                   $_id           the id of the document to retrieve from mongo
     * @param string                  $property      the property you are checking for
     * @param mixed                   $expectedValue if not null the property value will be matched against this expectedValue
     * @param Collection|IDriver|null $tripod        where to retrieve the document from
     */
    protected function assertDocumentHasProperty(array $_id, string $property, $expectedValue = null, $tripod = null): void
    {
        // just make sure $_id is aliased
        $labeller = new Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod);
        $this->assertNotNull($doc);

        $this->assertArrayHasKey($property, $doc, 'Document for ' . var_export($_id, true) . sprintf(' should have property [%s], but none found', $property));
        if ($expectedValue !== null) {
            $this->assertEquals($expectedValue, $doc[$property], sprintf('Document property [%s] actual value [', $property) . print_r($doc[$property], true) . '] does not match expected value [' . print_r($expectedValue, true) . ']');
        }
    }

    /**
     * @param array                   $_id                the id of the document to retrieve from mongo
     * @param string                  $property           the property you are checking for
     * @param Collection|IDriver|null $tripod             where to retrieve the document from
     * @param bool                    $fromTransactionLog if you want to retrieve the document from transaction log
     */
    protected function assertDocumentDoesNotHaveProperty(array $_id, string $property, $tripod = null, bool $fromTransactionLog = false): void
    {
        // just make sure $_id is aliased
        $labeller = new Labeller();
        foreach ($_id as $key => $value) {
            $_id[$key] = $labeller->uri_to_alias($value);
        }

        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        if ($doc === null) {
            return; // if document doesn't exist then it doesn't have the property, so assertion is successful
        }

        $this->assertArrayNotHasKey($property, $doc, 'Document for ' . var_export($_id, true) . sprintf(' should not have property [%s], but propert was found', $property));
    }

    /**
     * @param Collection|IDriver|null $tripod
     */
    protected function assertDocumentExists(array $_id, $tripod = null, bool $fromTransactionLog = false): void
    {
        $doc = $this->getDocument($_id, $tripod, $fromTransactionLog);
        $this->assertNotNull($doc);
        $this->assertEquals($_id, $doc['_id'], 'Actual Document _id :[' . print_r($doc['_id'], true) . '] did not match expected value of ' . print_r($_id, true));
    }

    /**
     * @param Driver|null $tripod
     */
    protected function assertDocumentHasBeenDeleted(array $_id, $tripod = null, bool $useTransactionTripod = false): void
    {
        $doc = $this->getDocument($_id, $tripod, $useTransactionTripod);
        if ($useTransactionTripod) {
            $this->assertNull($doc, 'Document with _id:[' . print_r($_id, true) . '] exists, but it should not');
        } else {
            $this->assertNotNull($doc, 'Document should exist');
            $keys = array_keys($doc);
            $this->assertCount(4, $keys);
            $this->assertArrayHasKey('_id', $doc);
            $this->assertArrayHasKey(_VERSION, $doc);
            $this->assertArrayHasKey(_CREATED_TS, $doc);
            $this->assertArrayHasKey(_UPDATED_TS, $doc);
        }
    }

    /**
     * @param string $o
     */
    protected function assertHasLiteralTriple(ExtendedGraph $graph, string $s, string $p, $o): void
    {
        $this->assertTrue($graph->has_literal_triple($s, $p, $o), sprintf('Graph did not contain the literal triple: <%s> <%s> "%s"', $s, $p, $o));
    }

    /**
     * @param string $o
     */
    protected function assertHasResourceTriple(ExtendedGraph $graph, string $s, string $p, $o): void
    {
        $this->assertTrue($graph->has_resource_triple($s, $p, $o), sprintf('Graph did not contain the resource triple: <%s> <%s> <%s>', $s, $p, $o));
    }

    /**
     * @param string $o
     */
    protected function assertDoesNotHaveLiteralTriple(ExtendedGraph $graph, string $s, string $p, $o): void
    {
        $this->assertFalse($graph->has_literal_triple($s, $p, $o), sprintf('Graph should not contain the literal triple: <%s> <%s> "%s"', $s, $p, $o));
    }

    /**
     * @param string $o
     */
    protected function assertDoesNotHaveResourceTriple(ExtendedGraph $graph, string $s, string $p, $o): void
    {
        $this->assertFalse($graph->has_resource_triple($s, $p, $o), sprintf('Graph should not contain the resource triple: <%s> <%s> <%s>', $s, $p, $o));
    }

    /**
     * @param string $transaction_id
     */
    protected function lockDocument(?string $subject, $transaction_id): void
    {
        $collection = Config::getInstance()->getCollectionForLocks('tripod_php_testing');
        $labeller = new Labeller();
        $doc = [
            '_id' => [_ID_RESOURCE => $labeller->uri_to_alias($subject), _ID_CONTEXT => Config::getInstance()->getDefaultContextAlias()],
            _LOCKED_FOR_TRANS => $transaction_id,
            _LOCKED_FOR_TRANS_TS => DateUtil::getMongoDate(),
        ];
        $collection->insertOne($doc, ['w' => 1]);
    }

    /**
     * @param string     $host
     * @param int|string $port
     * @param string     $prefix
     *
     * @return MockObject&StatsD
     */
    protected function getMockStat($host, $port, $prefix = '', array $mockedMethods = [])
    {
        $mockedMethods = array_merge(['send'], $mockedMethods);

        return $this->getMockBuilder(StatsD::class)
            ->onlyMethods($mockedMethods)
            ->setConstructorArgs([$host, $port, $prefix])
            ->getMock();
    }

    /**
     * @return array{class: class-string<StatsD>, config: array{host: string, port: int, prefix: string}}
     */
    protected function getStatsDConfig(): array
    {
        return [
            'class' => StatsD::class,
            'config' => [
                'host' => 'example.com',
                'port' => 1234,
                'prefix' => 'somePrefix',
            ],
        ];
    }

    private function getConfigLocation(): string
    {
        return __DIR__ . '/data/config.json';
    }

    private function getTlogCollection(): Collection
    {
        $config = Config::getInstance();
        $tLogConfig = $config->getTransactionLogConfig();

        return $config->getTransactionLogDatabase()->selectCollection($tLogConfig['collection']);
    }

    private function loadDataViaTripod(Driver $tripod, string $filename): void
    {
        $docs = json_decode((string) file_get_contents(__DIR__ . $filename), true);
        foreach ($docs as $d) {
            $g = new MongoGraph();
            $g->add_tripod_array($d);
            $tripod->saveChanges(new ExtendedGraph(), $g, $d['_id'][_ID_CONTEXT]);
        }
    }
}

class TestTripod extends Driver
{
    /**
     * @return ReadPreference
     */
    public function getCollectionReadPreference()
    {
        return $this->getCollection()->getReadPreference();
    }
}

class TripodTestConfig extends Tripod\Mongo\Config
{
    /**
     * Constructor.
     */
    public function __construct() {}

    public function loadConfig(array $config): void
    {
        parent::loadConfig($config);
    }
}
