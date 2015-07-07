<?php
set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__))))
        . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/lib'
        . PATH_SEPARATOR . dirname(dirname(dirname(dirname(__FILE__)))).'/src');

require_once dirname(__FILE__).'/../../unit/mongo/MongoTripodTestBase.php';
require_once('tripod.inc.php');

/**
 * A quick performance test to see what amount of time in consumed in specific methods of Config class
 *
 * Class MongoTripodConfigTest
 */
class LargeGraphTest extends MongoTripodTestBase
{
    /**
     * time in ms (milli-seconds) anything below which is acceptable.
     */
    const BENCHMARK_SAVE_TIME = 1000;

    /**
     * Do some setup before each test start
     */
    protected function setUp()
    {
        parent::setup();

        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";

        $this->tripod = new \Tripod\Mongo\Driver('CBD_testing','tripod_php_testing',array('defaultContext'=>'http://talisaspire.com/'));
    }

    protected function loadLargeGraphData()
    {
        $docs = json_decode(file_get_contents(dirname(__FILE__).'/data/largeGraph.json'), true);
        foreach ($docs as $d) {
            $this->addDocument($d);
        }
    }


    protected function getConfigLocation()
    {
        return dirname(__FILE__).'/../../unit/mongo/data/config.json';
    }

    /**
     * Note: Current version of this test tried to create 1000 objects within 6000ms which is reasonable at this time.
     *       Any change to this class if make it a more a big number it should be validated and tested to ensure performance impact.
     *
     * Create some instances of Config to see what amount of time is taken in creating instance and processing in constructor.
     */
    public function testUpdateSingleTripleOfLargeGraph()
    {
        $this->loadLargeGraphData();
        $uri = "http://largegraph/1";


        $testStartTime = microtime();

        $graph = new \Tripod\ExtendedGraph();
        $graph->add_literal_triple($uri,"http://rdfs.org/sioc/spec/name","new name");
        $this->tripod->saveChanges(new \Tripod\ExtendedGraph(),$graph);

        $testEndTime = microtime();

        $this->assertLessThan(
            self::BENCHMARK_SAVE_TIME,
            $this->getTimeDifference($testStartTime, $testEndTime),
            "It should always take less than " . self::BENCHMARK_SAVE_TIME . "ms to save a triple to a large graph"
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