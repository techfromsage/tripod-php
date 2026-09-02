<?php

declare(strict_types=1);

use Resque\JobHandler;
use Tripod\Config;
use Tripod\Mongo\DriverBase;

class JobBaseTest extends MongoTripodTestBase
{
    public function testLoggerInstance(): void
    {
        $this->assertSame(
            DriverBase::getLogger(),
            TestJobBase::getLogger(),
            'JobBase should return the same logger instance as DriverBase'
        );
    }

    public function testGetTripodConfig(): void
    {
        $job = new TestJobBase();
        $job->args = $this->getArgs();
        $job->job = new JobHandler('queue', ['id' => uniqid()]);

        $this->assertInstanceOf(Tripod\Mongo\Config::class, $job->getTripodConfig());
    }

    /**
     * @return array<string, mixed[]|string>
     */
    private function getArgs(): array
    {
        return [
            'tripodConfig' => Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'changes' => ['http://example.com/resources/foo' => ['rdf:type', 'dct:title']],
            'operations' => [OP_VIEWS, OP_TABLES, OP_SEARCH],
            'contextAlias' => 'http://talisaspire.com/',
        ];
    }
}
