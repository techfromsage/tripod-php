<?php

/**
 * A quick performance test to see what amount of time in consumed in specific methods of Config class
 *
 * Class MongoTripodConfigTest
 */
class LargeGraphTest extends MongoTripodPerformanceTestBase
{
    /**
     * time in ms (milli-seconds) anything below which is acceptable.
     */
    private const BENCHMARK_SAVE_TIME = 100;

    /**
     * time in ms (milli-seconds) anything below which is acceptable.
     */
    private const BENCHMARK_DESCRIBE_TIME = 5000;

    /**
     * Do some setup before each test start
     */
    protected function setUp(): void
    {
        parent::setup();
        $this->tripod = new Tripod\Mongo\Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']);

        $this->loadLargeGraphData();
    }

    protected function loadLargeGraphData()
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__) . '/data/largeGraph.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }

    protected function getConfigLocation()
    {
        return dirname(__FILE__) . '/../../unit/mongo/data/config.json';
    }

    public function testUpdateSingleTripleOfLargeGraph()
    {
        $uri = 'http://largegraph/1';

        $testStartTime = microtime();

        $graph = new Tripod\ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://rdfs.org/sioc/spec/name', 'new name');
        $this->tripod->saveChanges(new Tripod\ExtendedGraph(), $graph);

        $testEndTime = microtime();

        $this->assertLessThan(
            self::BENCHMARK_SAVE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            'It should always take less than ' . self::BENCHMARK_SAVE_TIME . 'ms to save a triple to a large graph'
        );
    }

    public function testDescribeOfLargeGraph()
    {
        $uri = 'http://largegraph/1';

        $testStartTime = microtime();

        $graph = new Tripod\ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://rdfs.org/sioc/spec/name', 'new name');
        $this->tripod->describeResource($uri);

        $testEndTime = microtime();

        $this->assertLessThan(
            self::BENCHMARK_DESCRIBE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            'It should always take less than ' . self::BENCHMARK_DESCRIBE_TIME . 'ms to describe large graph'
        );
    }
}
