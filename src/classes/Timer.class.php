<?php
class Timer
{
    /**
     * @var string, start time of event as returned from php microtime()
     */
    private $start_time = NULL;

    /**
     * @var string, end time of event as returned from php microtime()
     */
    private $end_time = NULL;

    /**
     * @var int, difference in milliseconds of event's start time and end time
     */
    private $result = NULL;

    /**
     * @var int, difference in micro-seconds of event's start time and end time
     */
    private $micro_result = NULL;

    /**
     * Captures current microtime as time of start. Call this before start of event
     */
    public function start()
    {
        $this->start_time = $this->getMicrotime();
    }

    /**
     * Captures current microtime as end time. Call this as soon as event execution is complete.
     */
    public function stop()
    {
        $this->end_time = $this->getMicrotime();
    }

    /**
     * Calculate difference between start and end time of event and return in milli-seconds.
     * @return number time difference in milliseconds between stat and end time of event
     * @throws Exception, if either of or both  of start or stop method are not called before this method
     */
    public function result()
    {
        if (is_null($this->start_time))
        {
            throw new Exception('Timer: start method not called !');
        }
        else if (is_null($this->end_time))
        {
            throw new Exception('Timer: stop method not called !');
        }

        if ($this->result==null)
        {
            list($endTimeMicroSeconds, $endTimeSeconds) = explode(' ', $this->end_time);
            list($startTimeMicroSeconds, $startTimeSeconds) = explode(' ', $this->start_time);

            $differenceInMilliSeconds =  ((float)$endTimeSeconds - (float)$startTimeSeconds)*1000;

            $this->result = round(($differenceInMilliSeconds + ((float)$endTimeMicroSeconds *1000)) -  (float)$startTimeMicroSeconds *1000);
        }
        return $this->result;
    }

    /**
     * Calculate difference between start and end time of event and return in micro-seconds.
     * @return number time difference in micro seconds between stat and end time of event
     * @throws Exception, if either of or both  of start or stop method are not called before this method
     */
    public function microResult()
    {
        if (is_null($this->start_time))
        {
            throw new Exception('Timer: start method not called !');
        }
        else if (is_null($this->end_time))
        {
            throw new Exception('Timer: stop method not called !');
        }

        if ($this->micro_result==null)
        {
            list($endTimeMicroSeconds, $endTimeSeconds) = explode(' ', $this->end_time);
            list($startTimeMicroSeconds, $startTimeSeconds) = explode(' ', $this->start_time);

            $differenceInMicroSeconds =  ((float)$endTimeSeconds - (float)$startTimeSeconds)*1000000;

            $this->micro_result = round(($differenceInMicroSeconds + ((float)$endTimeMicroSeconds *1000000)) -  (float)$startTimeMicroSeconds *1000000);
        }
        return $this->micro_result;
    }

    /**
     * @return string current system time in pair of seconds microseconds.
     */
    private function getMicrotime()
    {
        return microtime();
    }
}