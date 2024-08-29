<?php

use Tripod\Mongo\Jobs\JobBase;

class ResqueJobTestBase extends MongoTripodTestBase
{
    protected function performJob(JobBase $job)
    {
        $mockJob = $this->getMockBuilder(\Resque_Job::class)
            ->onlyMethods(['getInstance', 'getArguments'])
            ->setConstructorArgs(['test', get_class($job), $job->args])
            ->getMock();
        $mockJob->expects($this->atLeastOnce())
            ->method('getInstance')
            ->will($this->returnValue($job));
        $mockJob->expects($this->any())
            ->method('getArguments')
            ->will($this->returnValue($job->args));
        $mockJob->perform();
    }
}
