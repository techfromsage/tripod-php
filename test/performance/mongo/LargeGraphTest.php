<?php

declare(strict_types=1);

use Tripod\ExtendedGraph;
use Tripod\Mongo\Driver;

/**
 * A quick performance test to see what amount of time in consumed in specific methods of Config class.
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
     * Do some setup before each test start.
     */
    protected function setUp(): void
    {
        parent::setup();
        $this->tripod = new Driver('CBD_testing', 'tripod_php_testing', ['defaultContext' => 'http://talisaspire.com/']);

        $this->loadLargeGraphData();
    }

    public function testUpdateSingleTripleOfLargeGraph(): void
    {
        $uri = 'http://largegraph/1';

        $testStartTime = microtime();

        $this->startProfiler();

        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://rdfs.org/sioc/spec/name', 'new name');

        $this->tripod->saveChanges(new ExtendedGraph(), $graph);

        $testEndTime = microtime();

        $this->assertLessThan(
            self::BENCHMARK_SAVE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            'It should always take less than ' . self::BENCHMARK_SAVE_TIME . 'ms to save a triple to a large graph'
        );
    }

    public function testDescribeOfLargeGraph(): void
    {
        $uri = 'http://largegraph/1';

        $testStartTime = microtime();

        $this->startProfiler();

        $graph = new ExtendedGraph();
        $graph->add_literal_triple($uri, 'http://rdfs.org/sioc/spec/name', 'new name');

        $this->tripod->describeResource($uri);

        $testEndTime = microtime();

        $this->assertLessThan(
            self::BENCHMARK_DESCRIBE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            'It should always take less than ' . self::BENCHMARK_DESCRIBE_TIME . 'ms to describe large graph'
        );
    }

    private function loadLargeGraphData(): void
    {
        $docs = $this->decodeJsonFile(__DIR__ . '/data/largeGraph.json');
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }
}
