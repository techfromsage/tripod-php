<?php

namespace Tripod;

interface ITripodConfigSerializer
{

    /**
     * This should return an array that self::deserialize() can roundtrip into an Tripod Config object
     *
     * @return void
     */
    public function serialize();

    /**
     * When given a valid config, returns a Tripod Config object
     *
     * @param array $config
     * @return \Tripod\ITripodConfig
     */
    public static function deserialize(array $config);
}
