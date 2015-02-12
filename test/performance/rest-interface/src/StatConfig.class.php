<?php


class StatConfig {
    protected static $config;
    protected static $instance;
    protected $statsHost;
    protected $statsPort;
    private function __construct(){}

    public static function getInstance()
    {
        if(!isset(self::$instance))
        {
            self::$instance = new self();
        }
    }

    public static function setConfig(array $config)
    {
        self::$config = $config;
    }

}