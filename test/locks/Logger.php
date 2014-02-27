<?php

final class Logger
{
    protected static $instance = NULL;
    const LOG_FILE_PATH = '/tmp/tripod-locks.log';

    public static function getLogger()
    {
        if (self::$instance == NULL || !self::$instance instanceof Logger)
        {
            self::$instance = new self();
        }

        return self::$instance;
    }

    public static function getInstance()
    {
        return self::getLogger();
    }


    public function critical($message, $arrParams = NULL)
    {
        return $this->logMessage($message, $arrParams, 'critical');
    }

    /**
     *
     * Logs a message with log level 'error'
     * @param String $message
     * @param Array $arrParams
     */
    public function error($message, $arrParams = NULL)
    {
        return $this->logMessage($message, $arrParams, 'error');
    }

    /**
     *
     * Logs a message with log level 'warning'
     * @param String $message
     * @param Array $arrParams
     */
    public function warning($message, $arrParams = NULL)
    {
        return $this->logMessage($message, $arrParams, 'warning');
    }

    /**
     *
     * Logs a message with log level 'information'
     * @param String $message
     * @param Array $arrParams
     */
    public function information($message, $arrParams = NULL)
    {
        return $this->logMessage($message, $arrParams, 'information');
    }

    /**
     *
     * Logs a message with log level 'debug'
     * @param String $message
     * @param Array $arrParams
     */
    public function debug($message, $arrParams = NULL)
    {
        return $this->logMessage($message, $arrParams, 'debug');
    }

    protected function logMessage($message, $arrParams, $logLevel)
    {
        $message = "\n" . $message;

        foreach($arrParams as $key => $value){
            $message .= " -> " . $key . " : " .  $value;
        }

        error_log($message, 3, self::LOG_FILE_PATH);
    }
}