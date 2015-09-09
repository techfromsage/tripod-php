<?php

namespace Tripod;

/**
 * Class ITripodStat
 * @package Tripod
 */
interface ITripodStat
{
    /**
     * @param string $operation
     * @return mixed
     */
    public function increment($operation);

    /**
     * @param string $operation
     * @param number $duration
     * @return mixed
     */
    public function timer($operation,$duration);

    /**
     * A custom metric defined by the Stat class
     *
     * @param string $function The internal function name to use to record the stat
     * @param string $operation
     * @param mixed $value
     * @return mixed
     */
    public function custom($function, $operation, $value);

    /**
     * @return array
     */
    public function getConfig();

    /**
     * @param array $config
     * @return ITripodStat
     */
    public static function createFromConfig(array $config);
}