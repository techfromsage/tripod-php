<?php

use Tripod\Mongo\Jobs\JobBase;

trait PerformJob
{
    protected function performJob(JobBase $job)
    {
        $job->beforePerform();
        $job->perform();
        $job->afterPerform();
    }
}
