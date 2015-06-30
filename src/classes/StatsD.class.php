<?php

namespace Tripod;

/**
 * Class StatsD
 * @package Tripod
 */
class StatsD implements ITripodStat
{
    private $host;
    private $port;

    function __construct($host, $port, $prefix='')
    {
        $this->host = $host;
        $this->port = $port;
        $this->prefix = $prefix;
    }

    /**
     * @param string $operation
     * @return mixed
     */
    public function increment($operation)
    {
        $key = (empty($this->prefix)) ? $operation : "{$this->prefix}.$operation";
        $this->send(
            array($key=>"1|c")
        );
    }

    /**
     * @param string $operation
     * @param number $duration
     * @return mixed
     */
    public function timer($operation, $duration)
    {
        $key = (empty($this->prefix)) ? $operation : "{$this->prefix}.$operation";
        $this->send(
            array($key=>array("1|c","$duration|ms"))
        );
    }

    /**
     * Sends the stat(s) using UDP protocol
     * @param $data
     * @param int $sampleRate
     */
    protected function send($data, $sampleRate=1) {
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
        try
        {
            if (!empty($this->host)) // if host is configured, send..
            {
                $fp = fsockopen("udp://{$this->host}", $this->port);
                if (! $fp) { return; }
                foreach ($sampledData as $stat => $value)
                {
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
        }
        catch (\Exception $e)
        {
        }
    }


}
