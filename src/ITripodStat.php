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
     * @param int|number $inc Amount to increment by
     * @return mixed
     */
    public function increment($operation, $inc=1);

    /**
     * @param string $operation
     * @param number $duration
     * @return mixed
     */
    public function timer($operation,$duration);

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