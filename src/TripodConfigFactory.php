<?php

namespace Tripod;

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
        if (\Tripod\Config::getConfig() !== $config) {
            \Tripod\Config::setConfig($config);
        }
        if (isset($config['class']) && class_exists($config['class'])) {
            $tripodConfig = call_user_func(array($config['class'], 'deserialize'), $config);
        } else {
            $tripodConfig = \Tripod\Mongo\Config::deserialize($config);
        }
        return $tripodConfig;
    }
}
