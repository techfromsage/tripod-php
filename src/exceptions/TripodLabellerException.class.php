<?php
require_once TRIPOD_DIR . '/exceptions/TripodException.class.php';

/**
 * @codeCoverageIgnore
 */
class TripodLabellerException extends TripodException {
    private $target;
    public function __construct($target)
    {
        $this->target = $target;
        parent::__construct("Could not label: $target");
    }
    public function getTarget()
    {
        return $this->target;
    }
}