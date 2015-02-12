<?php

class Stat implements ITripodStat {

    private $tenantShortcode = null;

    public static $logger;

    function __construct($tenantShortcode)
    {
        $this->tenantShortcode = $tenantShortcode;
    }

    public function increment($operation)
    {
        self::trackTripodOp($operation, $this->tenantShortcode);
    }

    public function timer($operation,$duration)
    {
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
        self::trackTripodOp($operation,$this->tenantShortcode,$duration);
    }

    public static function trackTripodOp($stat,$tenantShortcode,$duration=null)
    {
        Stat::track($stat,"tripod","group_by_db",$tenantShortcode,$duration);
    }

    protected static function track($stat,$class,$pivotField,$pivotValue,$duration=null,$data=array())
    {
// What is this doing?
//        $data[LIST_APP.".$class.$pivotField.$pivotValue.$stat"] = "1|c";
//        if ($duration==null)
//        {
//            $data[LIST_APP.".$class.$stat"] = "1|c";
//        }
//        else
//        {
//            $data[LIST_APP.".$class.$stat"][] = "1|c";
//            $data[LIST_APP.".$class.$stat"][] = "$duration|ms";
//        }
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
            $host = 'localhost';
            $port = 8125;

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

}
?>