<?php

namespace Tripod\Mongo;

use \MongoDB\BSON\UTCDateTime;

class DateUtil
{
    /**
     * Return a UTCDateTime object
     * If you pass in your own time, it will use that to construct the object, otherwise
     * it will create an object based on the current time.
     * @param null $time
     * @return UTCDateTime
     */
    public static function getMongoDate($time = null) {
        if (is_null($time)) {
            $time = floor(microtime(true) * 1000);
        }
        return new UTCDateTime($time);
    }
}
