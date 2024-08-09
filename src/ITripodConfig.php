<?php

namespace Tripod;

interface ITripodConfig
{
    /**
     * Tripod Config instances are singletons, this method gets the existing or instantiates a new one.
     *
     * @return \Tripod\Mongo\IConfigInstance
     */
    public static function getInstance();

    /**
     * Loads the Tripod config into the instance
     *
     * @return void
     */
    public static function setConfig(array $config);

    /**
     * Returns the Tripod config array
     *
     * @return array
     */
    public static function getConfig();
}
