<?php

namespace Tripod\Exceptions;

require_once TRIPOD_DIR . '/exceptions/Exception.class.php';

/**
 * @codeCoverageIgnore
 */
class AsyncJobException extends Exception {

    /**
     * @param string $message
     */
    public function __construct($message)
    {
        parent::__construct("Async Job Exception: $message");
    }
}