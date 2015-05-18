<?php

namespace Tripod\Exceptions;

require_once TRIPOD_DIR . '/exceptions/Exception.class.php';

/**
 * @codeCoverageIgnore
 */
class LabellerException extends Exception {
    private $target;

    /**
     * @param string $target
     */
    public function __construct($target)
    {
        $this->target = $target;
        parent::__construct("Could not label: $target");
    }

    /**
     * @return string
     */
    public function getTarget()
    {
        return $this->target;
    }
}