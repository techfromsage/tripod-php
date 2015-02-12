<?php

require_once 'StatConfig.class.php';

define('TRIPOD_PERFORMANCE_TEST','TRIPOD_PERFORMANCE_TEST');
/**
 * Based on StatD class by Michael Grace http://geek.michaelgrace.org/2011/09/installing-statsd-on-ubuntu-server-10-04/
 */
class Stat implements ITripodStat {

    /**
     * @var string
     */
    protected $storeName;

    public static $logger;

    /**
     * @param string $storeName
     */
    function __construct($storeName)
    {
        $this->storeName = $storeName;
    }

    public function increment($operation)
    {
        self::trackTripodOp($operation,$this->storeName);
    }

    /**
     * Sends time spent on a operation to Statd. There is a check and error logging to only allows positive duration.
     * @param String $operation
     * @param Int $duration
     */
    public function timer($operation,$duration)
    {
        /**
         * Consider Followings before you make a change here:
         * 1. If $duration is negative here, Statd will fatal, so do not send negative duration.
         * 2. a) If you get so many error emails for invalid duration, there should be time difference between servers causing this,
         *    b) Time difference on servers can be an issue with items being processed queue processor,
         *       where start time is captured on server intercepting user request, end time is captured on server running queue processor.
         * 3. $duration will be 0 for some cases with MongoTripod::select queries where you do count or records does not exists or same query is repeated more than once in single request.
         */
        if($duration < 0) {
            $this->errorLog(
                "Stat::timer() failed with invalid duration value",
                array(
                    'operation' => $operation,
                    'duration' => $duration
                )
            );
            return;
        }
        self::trackTripodOp($operation,$this->storeName,$duration);
    }

    /**
     * Tracks operations and pivots by tenant short code (tenant short code is the mongo database name)
     * @static
     * @param string $stat
     * @param string $storeName
     * @param null $duration
     */
    public static function trackTripodOp($stat,$storeName,$duration=null)
    {
        Stat::track($stat,"tripod","group_by_db",$storeName,$duration);
    }


    /**
     * @static
     * @return object a Logger
     */
    public static function getLogger()
    {
        if (self::$logger)
        {
            return self::$logger;
        }
        else
        {
            return null;
        }
    }

    protected static function track($stat,$class,$pivotField,$pivotValue,$duration=null,$data=array())
    {
        $data[TRIPOD_PERFORMANCE_TEST.".$class.$pivotField.$pivotValue.$stat"] = "1|c";
        if ($duration==null)
        {
            $data[TRIPOD_PERFORMANCE_TEST.".$class.$stat"] = "1|c";
        }
        else
        {
            $data[TRIPOD_PERFORMANCE_TEST.".$class.$stat"][] = "1|c";
            $data[TRIPOD_PERFORMANCE_TEST.".$class.$stat"][] = "$duration|ms";
        }
        Stat::send($data);
    }

    private static function send($data, $sampleRate=1) {
        $sampledData = array();
        if ($sampleRate < 1) {
            foreach ($data as $stat => $value) {
                if ((mt_rand() / mt_getrandmax()) <= $sampleRate) {
                    $sampledData[$stat] = "$value|@$sampleRate";
                }
            }
        } else {
            $sampledData = $data;
        }
        if (empty($sampledData)) { return; }
        try {
            $config = StatConfig::getInstance();
            $host = $config->getStatsHost();
            $port = $config->getStatsPort();

            if (!empty($host)) // if host is configured, send..
            {
                $fp = fsockopen("udp://$host", $port, $errno, $errstr);
                if (! $fp) { return; }
                foreach ($sampledData as $stat => $value) {
                    if (is_array($value))
                    {
                        foreach ($value as $v)
                        {
                            fwrite($fp, "$stat:$v");
                        }
                    }
                    else
                    {
                        fwrite($fp, "$stat:$value");
                    }
                }
                fclose($fp);
            }
        } catch (Exception $e) {
        }
    }

    /**
     * Logs error to log file if logger initialised otherwise echo them to STDERR
     * @param string $message
     * @param array $params
     */
    private function errorLog($message, $params=array())
    {
        if (self::getLogger()!=null)
        {
            self::getLogger()->getInstance()->error("[STAT_ERR] $message",$params);
        }
        else
        {
            echo "$message\n";
            if (count($params))
            {
                echo "Params: \n";
                foreach ($params as $key=>$value)
                {
                    echo "$key: $value\n";
                }
            }
        }
    }
}
?>