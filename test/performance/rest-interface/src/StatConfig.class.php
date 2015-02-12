<?php


class StatConfig {
    /**
     * @var array
     */
    protected static $config = array();
    /**
     * @var StatConfig
     */
    protected static $instance;
    /**
     * @var string
     */
    protected $statsHost;
    /**
     * @var string
     */
    protected $statsPort;
    private function __construct(){}

    /**
     * @return StatConfig|null
     */
    public static function getInstance()
    {
        if(!isset(self::$instance))
        {
            if(empty(self::$config))
            {
                return null;
            }
            self::$instance = new self();
            self::$instance->loadConfig(self::$config);

        }
        return self::$instance;
    }

    /**
     * @param array $config
     */
    public static function setConfig(array $config)
    {
        self::$config = $config;
    }

    /**
     * @param array $config
     */
    protected function loadConfig(array $config)
    {
        if(isset($config['statsHost']))
        {
            $this->statsHost = $config['statsHost'];
        }
        if(isset($config['statsPort']))
        {
            $this->statsHost = $config['statsPort'];
        }
    }

    /**
     * @return string
     */
    public function getStatsHost()
    {
        return $this->statsHost;
    }

    /**
     * @return string
     */
    public function getStatsPort()
    {
        return $this->statsPort;
    }


}