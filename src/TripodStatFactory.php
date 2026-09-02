<?php

declare(strict_types=1);

namespace Tripod;

class TripodStatFactory
{
    /**
     * @return ITripodStat
     *
     * @throws Exceptions\ConfigException if the configured class does not provide a callable createFromConfig() method
     */
    public static function create(array $config = [])
    {
        if (isset($config['class']) && class_exists($config['class'])) {
            $factory = [$config['class'], 'createFromConfig'];
            if (!is_callable($factory)) {
                throw new Exceptions\ConfigException($config['class'] . ' does not provide a callable createFromConfig() method');
            }

            $stat = call_user_func($factory, $config);
            if (!$stat instanceof ITripodStat) {
                throw new Exceptions\ConfigException($config['class'] . '::createFromConfig() did not return an ITripodStat');
            }

            return $stat;
        }

        return NoStat::createFromConfig($config);
    }
}
