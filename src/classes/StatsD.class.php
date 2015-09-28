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
        $this->prefix = $prefix;
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
        $key = (empty($this->prefix)) ? $operation : "{$this->prefix}.$operation";
        $this->send(
            array($key=>$value."|g")
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
     */
    public function setPrefix($prefix)
    {
        $this->prefix = $prefix;
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
     * @param string $operation
     * @param string|array $value
     * @return array
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
     */
    public function setPivotValue($pivotValue)
    {
        $this->pivotValue = $pivotValue;
    }

    /**
     * @return array
     */
    protected function getStatsPaths()
    {
        return(array_values(array_filter($this->getStoreStatPath(), $this->getAggregateStatPath())));
    }

    /**
     * @return null|string
     */
    protected function getStoreStatPath()
    {
        $path = null;
        if(isset($this->pivotValue))
        {
            if(!empty($this->prefix))
            {
                $path = $this->prefix . '.';
            }
            $path .= STAT_CLASS . '.' . STAT_PIVOT_FIELD . '.' . $this->pivotValue;
        }
        return $path;
    }

    /**
     * @return string
     */
    protected function getAggregateStatPath()
    {
        return (empty($this->prefix) ? STAT_CLASS : $this->prefix . '.' . STAT_CLASS);
    }
}
