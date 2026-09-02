<?php

declare(strict_types=1);

use Resque\JobHandler;
use Tripod\Config;
use Tripod\ExtendedGraph;
use Tripod\Mongo\Composites\Views;
use Tripod\Mongo\Driver;
use Tripod\Mongo\DriverBase;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\ApplyOperation;
use Tripod\Mongo\Jobs\DiscoverImpactedSubjects;
use Tripod\Mongo\Jobs\JobBase;
use Tripod\Mongo\Updates;

class ConfigGeneratorTest extends MongoTripodTestBase
{
    private array $config = [];

    protected function setUp(): void
    {
        $this->config = [
            'class' => 'TestConfigGenerator',
            'filename' => __DIR__ . '/data/config.json',
        ];
        Config::setConfig($this->config);
    }

    public function testCreateFromConfig(): void
    {
        $instance = Config::getInstance();
        $this->assertInstanceOf(TestConfigGenerator::class, $instance);
        $this->assertSame(
            ['CBD_testing', 'CBD_test_related_content', 'CBD_testing_2'],
            $instance->getPods('tripod_php_testing')
        );
    }

    public function testSerializeConfig(): void
    {
        /** @var TestConfigGenerator $instance */
        $instance = Config::getInstance();
        $this->assertEquals($this->config, $instance->serialize());
    }

    public function testLoggerInstance(): void
    {
        $this->assertSame(
            DriverBase::getLogger(),
            Config::getInstance()::getLogger(),
            'Config instance should return the same logger instance as DriverBase'
        );
    }

    public function testConfigGeneratorsSerializedInDiscoverJobs(): void
    {
        $originalGraph = new ExtendedGraph();
        $originalGraph->add_resource_triple('http://example.com/1', RDF_TYPE, RDFS_CLASS);

        $newGraph = new ExtendedGraph();
        $newGraph->add_resource_triple('http://example.com/1', RDF_TYPE, OWL_CLASS);

        $subjectsAndPredicatesOfChange = ['http://example.com/1' => [RDF_TYPE]];

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getDataUpdater'])
            ->setConstructorArgs(
                ['CBD_testing', 'tripod_php_testing']
            )
            ->getMock();

        $updates = $this->getMockBuilder(Updates::class)
            ->onlyMethods(
                [
                    'applyHooks',
                    'storeChanges',
                    'setReadPreferenceToPrimary',
                    'processSyncOperations',
                    'getDiscoverImpactedSubjects',
                    'resetOriginalReadPreference',
                ]
            )
            ->setConstructorArgs([$tripod])
            ->getMock();

        $discoverJob = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['createJob'])
            ->getMock();

        $tripod->expects($this->once())->method('getDataUpdater')->willReturn($updates);
        $updates->expects($this->once())->method('getDiscoverImpactedSubjects')->willReturn($discoverJob);

        $updates->expects($this->once())->method('storeChanges')->willReturn(
            ['transaction_id' => uniqid(), 'subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange]
        );

        $discoverJob->expects($this->once())->method('createJob')
            ->with([
                'changes' => $subjectsAndPredicatesOfChange,
                'operations' => [OP_TABLES, OP_SEARCH],
                'storeName' => 'tripod_php_testing',
                'podName' => 'CBD_testing',
                'contextAlias' => 'http://talisaspire.com/',
                'statsConfig' => [],
            ]);

        $tripod->saveChanges(
            $originalGraph,
            $newGraph
        );
    }

    public function testSerializedConfigGeneratorsSentToApplyJobs(): void
    {
        $subjectsAndPredicatesOfChange = ['http://example.com/1' => [RDF_TYPE]];
        $impactedSubjects = [
            new ImpactedSubject(
                [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                ['v_resource_full']
            ),
        ];
        $jobArgs = [
            DiscoverImpactedSubjects::STORE_NAME_KEY => 'tripod_php_testing',
            DiscoverImpactedSubjects::POD_NAME_KEY => 'CBD_testing',
            DiscoverImpactedSubjects::CHANGES_KEY => $subjectsAndPredicatesOfChange,
            DiscoverImpactedSubjects::OPERATIONS_KEY => [OP_VIEWS],
            DiscoverImpactedSubjects::CONTEXT_ALIAS_KEY => 'http://talisaspire.com/',
            JobBase::TRIPOD_CONFIG_GENERATOR => $this->config,
        ];

        $tripod = $this->getMockBuilder(Driver::class)
            ->onlyMethods(['getComposite'])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        $views = $this->getMockBuilder(Views::class)
            ->onlyMethods(['getImpactedSubjects'])
            ->disableOriginalConstructor()
            ->getMock();

        $tripod->expects($this->once())->method('getComposite')
            ->with(OP_VIEWS)
            ->willReturn($views);

        $views->expects($this->once())->method('getImpactedSubjects')->willReturn($impactedSubjects);

        $discoverJob = $this->getMockBuilder(DiscoverImpactedSubjects::class)
            ->onlyMethods(['getTripod', 'getApplyOperation'])
            ->getMock();

        $applyJob = $this->getMockBuilder(ApplyOperation::class)
            ->onlyMethods(['submitJob'])
            ->setMockClassName('ApplyOperation_TestConfigGenerator')
            ->getMock();
        $discoverJob->args = $jobArgs;
        $discoverJob->job = new JobHandler('discover_queue', ['id' => uniqid()]);
        $discoverJob->expects($this->once())->method('getTripod')->willReturn($tripod);
        $discoverJob->expects($this->once())->method('getApplyOperation')->willReturn($applyJob);
        $configInstance = Config::getInstance();
        $applyJob->expects($this->once())->method('submitJob')
            ->with(
                $configInstance::getApplyQueueName(),
                'ApplyOperation_TestConfigGenerator',
                [
                    ApplyOperation::SUBJECTS_KEY => [
                        [
                            'resourceId' => [
                                _ID_RESOURCE => 'http://example.com/1',
                                _ID_CONTEXT => 'http://talisaspire.com/',
                            ],
                            'operation' => OP_VIEWS,
                            'specTypes' => ['v_resource_full'],
                            'storeName' => 'tripod_php_testing',
                            'podName' => 'CBD_testing',
                        ],
                    ],
                    JobBase::TRIPOD_CONFIG_GENERATOR => $this->config,
                ]
            );
        $discoverJob->setUp();
        $discoverJob->perform();
    }
}
