<?php

namespace Tripod;

use \Tripod\Mongo\Config;

class TripodConfigFactory
{
    /**
     * Factory method to get a Tripod\ITripodConfig instance from either a config array, or a serialized
     * ITripodConfigSerializer instance
     *
     * @param array $config The Tripod config or serialized ITripodConfigSerializer array
     * @return \Tripod\ITripodConfig
     */
    public static function create(array $config)
    {
        if (isset($config['class']) && class_exists($config['class'])) {
            $tripodConfig = call_user_func(array($config['class'], 'deserialize'), $config);
        } else {
            Config::setConfig($config);
            $tripodConfig = Config::getInstance();
        }
        return $tripodConfig;
    }
}
