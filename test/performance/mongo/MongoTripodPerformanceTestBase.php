<?php
require_once dirname(__FILE__) . '/../../../vendor/autoload.php';
require_once dirname(__FILE__) . '/../../../src/tripod.inc.php';
require_once dirname(__FILE__).'/../../unit/mongo/MongoTripodTestBase.php';


abstract class MongoTripodPerformanceTestBase extends MongoTripodTestBase
{
    /**
     * Helper function to calculate difference between two microtime values
     * @param $microTime1, time string from microtime()
     * @param $microTime2, time string from microtime()
     * @return float, time difference in ms
     */
    protected function getTimeDifference($microTime1, $microTime2){
        list($endTimeMicroSeconds, $endTimeSeconds) = explode(' ', $microTime2);
        list($startTimeMicroSeconds, $startTimeSeconds) = explode(' ', $microTime1);

        $differenceInMilliSeconds =  ((float)$endTimeSeconds - (float)$startTimeSeconds)*1000;

        return round(($differenceInMilliSeconds + ((float)$endTimeMicroSeconds *1000)) -  (float)$startTimeMicroSeconds *1000);
    }

}
