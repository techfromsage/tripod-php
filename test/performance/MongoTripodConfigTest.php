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
     * Used in capturing start time of test in micro-seconds
     * @var String
     */
    private $test_start_time = null;

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
        $this->test_start_time = microtime();
    }

    /**
     * Post test completion actions.
     * e.g. Calculate time taken by test.
     */
    protected function tearDown()
    {
        $this->config = array();
        $test_end_time  = microtime();

        list($endTimeMicroSeconds, $endTimeSeconds) = explode(' ', $test_end_time);
        list($startTimeMicroSeconds, $startTimeSeconds) = explode(' ', $this->test_start_time);

        $differenceInMilliSeconds =  ((float)$endTimeSeconds - (float)$startTimeSeconds)*1000;

        echo "\n". $this->getName(FALSE) . " : Total time taken (ms) : " . round(($differenceInMilliSeconds + ((float)$endTimeMicroSeconds *1000)) -  (float)$startTimeMicroSeconds *1000);
    }

    /**
     * Create some instances of TripodConfig to see what amount of time is taken in creating instance and processing in constructor.
     */
    public function testCreateMongoTripodConfigObject()
    {
        //Let's try to create 50 objects to see how much time they take.
        for($i =0; $i<50; $i++) {
            $instance = new MongoTripodConfig($this->config);
        }
    }
}