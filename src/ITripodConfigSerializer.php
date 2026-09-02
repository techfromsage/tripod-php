<?php

declare(strict_types=1);

namespace Tripod;

use Tripod\Mongo\IConfigInstance;

interface ITripodConfigSerializer
{
    /**
     * This should return an array that self::deserialize() can roundtrip into an Tripod Config object.
     */
    public function serialize(): array;

    /**
     * When given a valid config, returns a Tripod Config object.
     */
    public static function deserialize(array $config): IConfigInstance;
}
