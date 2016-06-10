<?php

namespace Tripod\Mongo\Composites;

/**
 * Interface ICompositeRegen
 * @package Tripod\Mongo\Composites
 */
interface ICompositeRegen
{
    /**
     * Regenerate a specific composite document from its spec and root CBD
     * @param $specification
     * @param $compositeCollection
     * @param $rootCbd
     * @return void
     */
    public function regenerateOne($specification, $compositeCollection, $rootCbd);
}
