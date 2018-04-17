<?php

namespace Tripod;

interface ITripodConfig
{
    /**
     * Tripod Config instances are singletons, this method gets the existing or instantiates a new one.
     *
     * @return ITripodConfig
     */
    public static function getInstance();

    /**
     * Loads the Tripod config into the instance
     *
     * @return void
     */
    public static function setConfig(array $config);
}
