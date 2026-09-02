<?php

declare(strict_types=1);

use Tripod\Config;

/**
 * A quick performance test to see what amount of time in consumed in specific methods of Config class.
 */
class MongoTripodConfigTest extends MongoTripodPerformanceTestBase
{
    /**
     * time in ms (milli-seconds) anything below which is acceptable.
     */
    private const BENCHMARK_OBJECT_CREATE_TIME = 6000;

    /**
     * Number of iterations should to be ran to test.
     */
    private const BENCHMARK_OBJECT_CREATE_ITERATIONS = 1000;

    /**
     * Holds tripod config.
     */
    private array $config = [];

    /**
     * Do some setup before each test start.
     */
    protected function setUp(): void
    {
        parent::setup();

        $this->config = $this->decodeJsonFile(__DIR__ . '/../../unit/mongo/data/config.json');
    }

    /**
     * Post test completion actions.
     */
    protected function tearDown(): void
    {
        $this->config = [];
        parent::tearDown();
    }

    /**
     * Note: Current version of this test tried to create 1000 objects within 6000ms which is reasonable at this time.
     *       Any change to this class if make it a more a big number it should be validated and tested to ensure performance impact.
     *
     * Create some instances of Config to see what amount of time is taken in creating instance and processing in constructor.
     */
    public function testCreateMongoTripodConfigObject(): void
    {
        $testStartTime = microtime();

        // Let's try to create 1000 objects to see how much time they take.
        for ($i = 0; $i < self::BENCHMARK_OBJECT_CREATE_ITERATIONS; $i++) {
            Config::setConfig($this->config);
            $instance = Config::getInstance();
        }

        $testEndTime = microtime();
        $this->assertLessThan(
            self::BENCHMARK_OBJECT_CREATE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            'It should always take less than ' . self::BENCHMARK_OBJECT_CREATE_TIME . 'ms to create ' . self::BENCHMARK_OBJECT_CREATE_ITERATIONS . ' objects of Config class'
        );
    }
}
