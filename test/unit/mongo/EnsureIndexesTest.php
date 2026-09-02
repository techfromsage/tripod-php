<?php

declare(strict_types=1);

use PHPUnit\Framework\MockObject\MockObject;
use Resque\JobHandler;
use Tripod\Config;
use Tripod\Exceptions\Exception;
use Tripod\Exceptions\JobException;
use Tripod\Mongo\IndexUtils;
use Tripod\Mongo\Jobs\EnsureIndexes;

class EnsureIndexesTest extends ResqueJobTestBase
{
    private array $args = [];

    protected function setUp(): void
    {
        $this->args = [
            'tripodConfig' => '',
            'storeName' => '',
            'reindex' => '',
            'background' => '',
        ];
        parent::setUp();
    }

    /**
     * Test exception is thrown if mandatory arguments are not set.
     *
     * @dataProvider mandatoryArgDataProvider
     *
     * @group ensure-indexes
     *
     * @throws Exception
     */
    public function testMandatoryArgs(string $argument, ?string $argumentName = null): void
    {
        if (!$argumentName) {
            $argumentName = $argument;
        }

        $job = new EnsureIndexes();
        $job->args = $this->args;
        $job->job = new JobHandler('queue', ['id' => uniqid()]);
        unset($job->args[$argument]);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(sprintf('Argument %s was not present in supplied job args for job %s', $argumentName, EnsureIndexes::class));
        $this->performJob($job);
    }

    /**
     * Data provider for testMandatoryArgs.
     */
    public function mandatoryArgDataProvider(): iterable
    {
        yield ['tripodConfig', 'tripodConfig or tripodConfigGenerator'];
        yield ['storeName'];
        yield ['reindex'];
        yield ['background'];
    }

    /**
     * Test beforePerform hook accepts legacy Resque job objects.
     *
     * @group ensure-indexes
     *
     * @throws Exception
     */
    public function testMandatoryArgsLegacy(): void
    {
        if (!class_exists(Resque_Job::class)) {
            $this->markTestSkipped('This test only applies to resque/php-resque pre-namespaces');
        }

        $job = new EnsureIndexes();
        $job->args = $this->args;
        $job->job = new Resque_Job('queue', ['id' => uniqid()]);
        unset($job->args['storeName']);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(sprintf('Argument %s was not present in supplied job args for job %s', 'storeName', EnsureIndexes::class));
        $this->performJob($job);
    }

    /**
     * Test the job behaves as expected.
     *
     * @group ensure-indexes
     */
    public function testSuccessfullyEnsureIndexesJob(): void
    {
        $job = $this->createMockJob();
        $job->args = $this->createDefaultArguments();
        $this->jobSuccessfullyEnsuresIndexes($job);

        $this->performJob($job);
    }

    /**
     * Test that the job fails by throwing an exception.
     *
     * @group ensure-indexes
     */
    public function testEnsureIndexesJobThrowsErrorWhenCreatingIndexes(): void
    {
        $job = $this->createMockJob();
        $job->args = $this->createDefaultArguments();
        $this->jobThrowsExceptionWhenEnsuringIndexes($job);
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Ensuring index failed');

        $this->performJob($job);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will use the default queue name.
     */
    public function testEnsureIndexesCreateJobDefaultQueue(): void
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        // create mock job
        $job = $this->createMockJob();
        $job->expects($this->once())
            ->method('submitJob')
            ->with(
                Tripod\Mongo\Config::getEnsureIndexesQueueName(),
                'MockEnsureIndexes',
                $jobData
            );

        $job->createJob('tripod_php_testing', false, true);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will throw the expected exception if redis is unreachable.
     */
    public function testEnsureIndexesCreateJobUnreachableRedis(): void
    {
        [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        // create mock job
        $job = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods(['warningLog', 'enqueue'])
            ->getMock();

        $e = new Exception('Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known');

        // this is called 6 times because after the first attempt fails it will
        // retry 5 times.
        $job->expects($this->exactly(6))->method('enqueue')->willThrowException($e);

        // expect 5 retries. Catch this with call to warning log
        $job->expects($this->exactly(5))->method('warningLog');

        $this->expectException(JobException::class);
        $this->expectExceptionMessage('Exception queuing job  - Connection to Redis failed after 1 failures.Last Error : (0) php_network_getaddresses: getaddrinfo failed: nodename nor servname provided, or not known');
        $job->createJob('tripod_php_testing', false, true);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will throw the expected exception if the job fails.
     */
    public function testEnsureIndexesCreateJobStatusFalse(): void
    {
        [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        $job = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods(['warningLog', 'enqueue', 'hasJobStatus'])
            ->getMock();

        // both of these methods will be called 6 times because after the first attempt fails it will
        // retry 5 times.
        $job->expects($this->exactly(6))->method('enqueue')->willReturn('sometoken');
        $job->expects($this->exactly(6))->method('hasJobStatus')->willReturn(false);

        // expect 5 retries. Catch this with call to warning log
        $job->expects($this->exactly(5))->method('warningLog');
        $this->expectException(JobException::class);
        $this->expectExceptionMessage('Exception queuing job  - Could not retrieve status for queued job - job sometoken failed to tripod::ensureindexes');
        $job->createJob('tripod_php_testing', false, true);
    }

    /**
     * test that calling the create job method on the ensureindexes job class
     * will use the queue name specified.
     */
    public function testEnsureIndexesCreateJobSpecifyQueue(): void
    {
        $jobData = [
            'storeName' => 'tripod_php_testing',
            'tripodConfig' => Config::getConfig(),
            'reindex' => false,
            'background' => true,
        ];

        $job = $this->createMockJob();

        $queueName = Tripod\Mongo\Config::getEnsureIndexesQueueName() . '::TRIPOD_TESTING_QUEUE_' . uniqid();

        $job->expects($this->once())
            ->method('submitJob')
            ->with(
                $queueName,
                'MockEnsureIndexes',
                $jobData
            );

        $job->createJob('tripod_php_testing', false, true, $queueName);
    }

    // HELPER METHODS BELOW HERE

    /**
     *  Creates a simple mock EnsureIndexes Job.
     *
     * @return EnsureIndexes&MockObject
     */
    /**
     * @param string[] $methods
     *
     * @return EnsureIndexes&MockObject
     */
    private function createMockJob(array $methods = ['getIndexUtils', 'submitJob', 'warningLog', 'enqueue', 'hasJobStatus']): EnsureIndexes
    {
        $mockEnsureIndexesJob = $this->getMockBuilder(EnsureIndexes::class)
            ->onlyMethods($methods)
            ->setMockClassName('MockEnsureIndexes')
            ->getMock();
        $mockEnsureIndexesJob->job = new JobHandler('queue', ['id' => uniqid()]);

        return $mockEnsureIndexesJob;
    }

    /**
     * Returns default arguments for a EnsureIndexes Job.
     *
     * @return array<string, bool|mixed[]|string>
     */
    private function createDefaultArguments(): array
    {
        return [
            'tripodConfig' => Config::getConfig(),
            'storeName' => 'tripod_php_testing',
            'reindex' => false,
            'background' => true,
        ];
    }

    /**
     * @param EnsureIndexes&MockObject $job EnsureIndexes Job
     */
    private function jobSuccessfullyEnsuresIndexes(EnsureIndexes $job): void
    {
        $mockIndexUtils = $this->getMockBuilder(IndexUtils::class)
            ->onlyMethods(['ensureIndexes'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true);

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->willReturn($mockIndexUtils);
    }

    /**
     * @param EnsureIndexes&MockObject $job EnsureIndexes Job
     */
    private function jobThrowsExceptionWhenEnsuringIndexes(EnsureIndexes $job): void
    {
        $mockIndexUtils = $this->getMockBuilder(IndexUtils::class)
            ->onlyMethods(['ensureIndexes'])
            ->getMock();

        $mockIndexUtils->expects($this->once())
            ->method('ensureIndexes')
            ->with(false, 'tripod_php_testing', true)
            ->willThrowException(new Exception('Ensuring index failed'));

        $job->expects($this->once())
            ->method('getIndexUtils')
            ->willReturn($mockIndexUtils);
    }
}
