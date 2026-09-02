<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Mongo\IConfigInstance;

class TripodConfigFactory
{
    /**
     * Factory method to get a Tripod config instance from either a config array, or a serialized
     * ITripodConfigSerializer instance.
     *
     * @param array<string, mixed> $config The Tripod config or serialized ITripodConfigSerializer array
     *
     * @throws Exceptions\ConfigException if the configured class does not provide a callable deserialize() method
     */
    public static function create(array $config): IConfigInstance
    {
        if (Config::getConfig() !== $config) {
            Config::setConfig($config);
        }

        if (isset($config['class']) && class_exists($config['class'])) {
            $deserializer = [$config['class'], 'deserialize'];
            if (!is_callable($deserializer)) {
                throw new Exceptions\ConfigException($config['class'] . ' does not provide a callable deserialize() method');
            }

            return call_user_func($deserializer, $config);
        }

        return Mongo\Config::deserialize($config);
    }
}
