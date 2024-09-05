<?php

class JobBaseTest extends MongoTripodTestBase
{
    public function testGetTripodConfig()
    {
        $job = new TestJobBase();
        $job->args = $this->getArgs();
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);

        $this->assertInstanceOf(Tripod\Mongo\IConfigInstance::class, $job->getTripodConfig());
    }

    protected function getArgs()
    {
        return [
            'tripodConfig' => Tripod\Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'podName' => 'CBD_testing',
            'changes' => ['http://example.com/resources/foo' => ['rdf:type', 'dct:title']],
            'operations' => [OP_VIEWS, OP_TABLES, OP_SEARCH],
            'contextAlias' => 'http://talisaspire.com/',
        ];
    }
}
