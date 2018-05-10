<?php

namespace Tripod;

class Config implements ITripodConfig
{
    /**
     * @var Config
     */
    private static $instance;

    /**
     * @var array
     */
    private static $config = [];

    /**
     * Config should not be instantiated directly: use Config::getInstance()
     */
    private function __construct()
    {
    }

    /**
     * Since this is a singleton class, use this method to create a new config instance.
     * @uses Config::setConfig() Configuration must be set prior to calling this method. To generate a completely new object, set a new config
     * @codeCoverageIgnore
     * @throws \Tripod\Exceptions\ConfigException
     * @return ITripodConfig
     */
    public static function getInstance()
    {
        if (!isset(self::$config)) {
            throw new \Tripod\Exceptions\ConfigException("Call Config::setConfig() first");
        }
        if (!isset(self::$instance)) {
            self::$instance = TripodConfigFactory::create(self::$config);
        }
        return self::$instance;
    }

    /**
     * Loads the Tripod config into the instance
     *
     * @return void
     */
    public static function setConfig(array $config)
    {
        self::$config = $config;
        self::$instance = null; // this will force a reload next time getInstance() is called
    }

    /**
     * Returns the Tripod config array
     *
     * @return array
     */
    public static function getConfig()
    {
        return self::$config;
    }

    /**
     * This method was added to allow us to test the getInstance() method
     * @codeCoverageIgnore
     */
    public static function destroy()
    {
        self::$instance = null;
        self::$config = null;
    }
}
