<?php

namespace Tripod;

/**
 * Class StatsD
 * @package Tripod
 */
class StatsD implements ITripodStat
{
    /** @var string  */
    private $host;
    /** @var int|string  */
    private $port;
    /** @var string  */
    private $prefix;
    /** @var  string */
    private $pivotValue;

    /**
     * @param string $host
     * @param string|int $port
     * @param string $prefix
     */
    public function __construct($host, $port, $prefix='')
    {
        $this->host = $host;
        $this->port = $port;
        $this->setPrefix($prefix);
    }

    /**
     * @param string $operation
     * @param int $inc
     * @return void
     */
    public function increment($operation, $inc=1)
    {
        $this->send(
            $this->generateStatData($operation, $inc."|c")
        );
    }

    /**
     * @param string $operation
     * @param number $duration
     * @return mixed
     */
    public function timer($operation, $duration)
    {
        $this->send(
            $this->generateStatData($operation, array("1|c","$duration|ms"))
        );
    }

    /**
     * Record an arbitrary value
     *
     * @param string $operation
     * @param mixed $value
     */
    public function gauge($operation, $value)
    {
        $this->send(
            $this->generateStatData($operation, $value.'|g')
        );
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return array(
            'class'=>get_class($this),
            'config'=>array(
                'host'=>$this->host,
                'port'=>$this->port,
                'prefix'=>$this->prefix
            )
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
                // make this a non blocking stream
                stream_set_blocking($fp, false);
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

    /**
     * @param array $config
     * @return StatsD
     */
    public static function createFromConfig(array $config)
    {
        if(isset($config['config']))
        {
            $config = $config['config'];
        }

        $host = (isset($config['host']) ? $config['host'] : null);
        $port = (isset($config['port']) ? $config['port'] : null);
        $prefix = (isset($config['prefix']) ? $config['prefix'] : '');
        return new self($host, $port, $prefix);
    }

    /**
     * @return string
     */
    public function getPrefix()
    {
        return $this->prefix;
    }

    /**
     * @param string $prefix
     * @throws \InvalidArgumentException
     */
    public function setPrefix($prefix)
    {
        if($this->isValidPathValue($prefix))
        {
            $this->prefix = $prefix;
        }
        else
        {
            throw new \InvalidArgumentException('Invalid prefix supplied');
        }
    }

    /**
     * @return int|string
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param int|string $port
     */
    public function setPort($port)
    {
        $this->port = $port;
    }

    /**
     * @return string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param string $host
     */
    public function setHost($host)
    {
        $this->host = $host;
    }

    /**
     * This method combines the by database and aggregate stats to send to StatsD.  The return will look something list:
     * {
     *  "{prefix}.tripod.group_by_db.{storeName}.{stat}"=>"1|c",
     *  "{prefix}.tripod.{stat}"=>"1|c"
     * }
     *
     * @param string $operation
     * @param string|array $value
     * @return array An associative array of the grouped_by_database and aggregate stats
     */
    protected function generateStatData($operation, $value)
    {
        $data = array();
        foreach($this->getStatsPaths() as $path)
        {
            $data[$path . ".$operation"]=$value;
        }
        return $data;
    }

    /**
     * @return string
     */
    public function getPivotValue()
    {
        return $this->pivotValue;
    }

    /**
     * @param string $pivotValue
     * @throws \InvalidArgumentException
     */
    public function setPivotValue($pivotValue)
    {
        if($this->isValidPathValue($pivotValue))
        {
            $this->pivotValue = $pivotValue;
        }
        else
        {
            throw new \InvalidArgumentException('Invalid pivot value supplied');
        }
    }

    /**
     * @return array
     */
    protected function getStatsPaths()
    {
        return(array_values(array_filter(array($this->getAggregateStatPath()))));
    }

    /**
     * @return string
     */
    protected function getAggregateStatPath()
    {
        return (empty($this->prefix) ? STAT_CLASS : $this->prefix . '.' . STAT_CLASS);
    }

    /**
     * StatsD paths cannot start with, end with, or have more than one consecutive '.'
     * @param $value
     * @return bool
     */
    protected function isValidPathValue($value)
    {
        return (preg_match("/(^\.)|(\.\.+)|(\.$)/", $value) === 0);
    }
}
