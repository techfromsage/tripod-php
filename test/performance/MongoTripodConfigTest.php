<?php
set_include_path(
    get_include_path()
    . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__)))
    . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/lib'
    . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'src/mongo/MongoTripodConfig.class.php';

/**
 * A quick performance test to see what amount of time in consumed in specific methods of MongoTripodConfig class
 *
 * Class MongoTripodConfigTest
 */
class MongoTripodConfigTest extends PHPUnit_Framework_TestCase
{
    /**
     * time in ms (milli-seconds) anything below which is acceptable.
     */
    const BENCHMARK_OBJECT_CREATE_TIME = 4000;

    /**
     * Number of iterations should to be ran to test
     */
    const BENCHMARK_OBJECT_CREATE_ITERATIONS = 1000;

    /**
     * Holds tripod config
     * @var array
     */
    private $config = array();


    /**
     * Do some setup before each test start
     */
    protected function setUp()
    {
        parent::setup();
        $this->config = json_decode(file_get_contents(dirname(__FILE__) . '/tripodConfig.json'), true);
    }

    /**
     * Post test completion actions.
     */
    protected function tearDown()
    {
        $this->config = array();
        parent::tearDown();
    }

    /**
     * Note: Current version of this test tried to create 1000 objects within 4000ms which is reasonable at this time.
     *       Any change to this class if make it a more a big number it should be validated and tested to ensure performance impact.
     *
     * Create some instances of TripodConfig to see what amount of time is taken in creating instance and processing in constructor.
     */
    public function testCreateMongoTripodConfigObject()
    {
        $testStartTime = microtime();

        //Let's try to create 1000 objects to see how much time they take.
        for($i =0; $i < self::BENCHMARK_OBJECT_CREATE_ITERATIONS; $i++) {
            $instance = new MongoTripodConfig($this->config);
        }

        $testEndTime = microtime();
        $this->$this->assertLessThan(
            self::BENCHMARK_TIME_OBJECT_CREATE,
            $this->getTimeDifference($testStartTime, $testEndTime),
            "It should always take less than " . self::BENCHMARK_TIME_OBJECT_CREATE . "ms to create " . self::BENCHMARK_OBJECT_CREATE_ITERATIONS . " objects of MongoTripodConfig class"
        );
    }

    /**
     * Helper function to calculate difference between two microtime values
     * @param $microTime1, time string from microtime()
     * @param $microTime2, time string from microtime()
     * @return float, time difference in ms
     */
    private function getTimeDifference($microTime1, $microTime2){
        list($endTimeMicroSeconds, $endTimeSeconds) = explode(' ', $microTime2);
        list($startTimeMicroSeconds, $startTimeSeconds) = explode(' ', $microTime1);

        $differenceInMilliSeconds =  ((float)$endTimeSeconds - (float)$startTimeSeconds)*1000;

        return round(($differenceInMilliSeconds + ((float)$endTimeMicroSeconds *1000)) -  (float)$startTimeMicroSeconds *1000);
    }
}