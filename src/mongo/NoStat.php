<?php

namespace Tripod\Mongo;

/**
 * Class NoStat
 * @package Tripod\Mongo
 */
final class NoStat implements \Tripod\ITripodStat
{
    /**
     * @var self
     */
    public static $instance = null;

    /**
     * @param string $operation
     * @param int|number $inc
     * @return void
     */
    public function increment($operation, $inc = 1)
    {
        // do nothing
    }

    /**
     * @param string $operation
     * @param number $duration
     * @return void
     */
    public function timer($operation, $duration)
    {
        // do nothing
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return array();
    }

    /**
     * @return self
     */
    public static function getInstance()
    {
        if (self::$instance == null)
        {
            self::$instance = new NoStat();
        }
        return self::$instance;
    }

    /**
     * @param array $config
     * @return NoStat
     */
    public static function createFromConfig(array $config = array())
    {
        return self::getInstance();
    }
}
