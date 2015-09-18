<?php


namespace Tripod;
use Tripod\Mongo\NoStat;

/**
 * Class TripodStatFactory
 * @package Tripod
 */
class TripodStatFactory
{

    /**
     * @param array $config
     * @return ITripodStat
     */
    public static function create(array $config = array())
    {
        if(isset($config['class']) && class_exists($config['class']))
        {
            return call_user_func(array($config['class'], 'createFromConfig'), $config);
        }
        else
        {
            return NoStat::createFromConfig($config);
        }
    }
}