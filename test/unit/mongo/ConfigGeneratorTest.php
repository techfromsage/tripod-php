<?php

use Tripod\Mongo\Jobs\DiscoverImpactedSubjects;
use Tripod\Mongo\Jobs\JobBase;
use Tripod\Mongo\ImpactedSubject;
use Tripod\Mongo\Jobs\ApplyOperation;

require_once 'MongoTripodTestBase.php';
require_once 'TestConfigGenerator.php';

class ConfigGeneratorTest extends MongoTripodTestBase
{
    private $config = [];

    public function setUp()
    {
        $this->config = [
            'class' => 'TestConfigGenerator',
            'filename' => dirname(__FILE__) . '/data/config.json'
        ];
        \Tripod\Config::setConfig($this->config);
    }
    public function testCreateFromConfig()
    {
        /** @var TestConfigGenerator $instance */
        $instance = \Tripod\Config::getInstance();
        $this->assertInstanceOf('TestConfigGenerator', $instance);
        $this->assertInstanceOf('\Tripod\Mongo\Config', $instance);
        $this->assertInstanceOf('\Tripod\ITripodConfigSerializer', $instance);
        $this->assertEquals(
            ['CBD_testing', 'CBD_testing_2'],
            $instance->getPods('tripod_php_testing')
        );
    }

    public function testSerializeConfig()
    {
        /** @var TestConfigGenerator $instance */
        $instance = \Tripod\Config::getInstance();
        $this->assertEquals($this->config, $instance->serialize());
    }

    public function testConfigGeneratorsSerializedInDiscoverJobs()
    {
        $timestamp = new \MongoDB\BSON\UTCDateTime(null);
        $originalGraph = new \Tripod\ExtendedGraph();
        $originalGraph->add_resource_triple('http://example.com/1', RDF_TYPE, RDFS_CLASS);

        $newGraph = new \Tripod\ExtendedGraph();
        $newGraph->add_resource_triple('http://example.com/1', RDF_TYPE, OWL_CLASS);
        $subjectsAndPredicatesOfChange = ['http://example.com/1' => [RDF_TYPE]];

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $tripod */
        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods(['getDataUpdater'])
            ->setConstructorArgs(
                ['CBD_testing', 'tripod_php_testing']
            )
            ->getMock();

        /** @var \Tripod\Mongo\Updates|PHPUnit_Framework_MockObject_MockObject $updates */
        $updates = $this->getMockBuilder('\Tripod\Mongo\Updates')
            ->setMethods(
                [
                    'applyHooks',
                    'storeChanges',
                    'setReadPreferenceToPrimary',
                    'processSyncOperations',
                    'getDiscoverImpactedSubjects',
                    'resetOriginalReadPreference',
                    'getMongoDate'
                ]
            )
            ->setConstructorArgs([$tripod])
            ->getMock();

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverJob */
        $discoverJob = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(['createJob'])
            ->getMock();

        $tripod->expects($this->once())->method('getDataUpdater')->will($this->returnValue($updates));
        $updates->expects($this->once())->method('getDiscoverImpactedSubjects')->will($this->returnValue($discoverJob));
        $updates->expects($this->atLeastOnce())->method('getMongoDate')->will($this->returnValue($timestamp));

        $updates->expects($this->once())->method('storeChanges')->will(
            $this->returnValue(
                ['transaction_id' => uniqid(), 'subjectsAndPredicatesOfChange' => $subjectsAndPredicatesOfChange]
            )
        );

        $discoverJob->expects($this->once())->method('createJob')
            ->with([
                'changes' => $subjectsAndPredicatesOfChange,
                'operations' => [OP_TABLES, OP_SEARCH],
                'storeName' => 'tripod_php_testing',
                'podName' => 'CBD_testing',
                'contextAlias' => 'http://talisaspire.com/',
                'statsConfig' => [],
                'timestamp' => $timestamp
            ]);

        $tripod->saveChanges(
            $originalGraph,
            $newGraph
        );
    }

    public function testSerializedConfigGeneratorsSentToApplyJobs()
    {
        $subjectsAndPredicatesOfChange = ['http://example.com/1' => [RDF_TYPE]];
        $impactedSubjects = [
            new ImpactedSubject(
                [_ID_RESOURCE => 'http://example.com/1', _ID_CONTEXT => 'http://talisaspire.com/'],
                OP_VIEWS,
                'tripod_php_testing',
                'CBD_testing',
                ['v_resource_full']
            )
        ];
        $jobArgs = [
            DiscoverImpactedSubjects::STORE_NAME_KEY => 'tripod_php_testing',
            DiscoverImpactedSubjects::POD_NAME_KEY => 'CBD_testing',
            DiscoverImpactedSubjects::CHANGES_KEY => $subjectsAndPredicatesOfChange,
            DiscoverImpactedSubjects::OPERATIONS_KEY => [OP_VIEWS],
            DiscoverImpactedSubjects::CONTEXT_ALIAS_KEY => 'http://talisaspire.com/',
            JobBase::TRIPOD_CONFIG_GENERATOR => $this->config
        ];

        /** @var \Tripod\Mongo\Driver|PHPUnit_Framework_MockObject_MockObject $tripod */
        $tripod = $this->getMockBuilder('\Tripod\Mongo\Driver')
            ->setMethods([])
            ->setConstructorArgs(['CBD_testing', 'tripod_php_testing'])
            ->getMock();

        /** @var \Tripod\Mongo\Composites\Views|PHPUnit_Framework_MockObject_MockObject $views */
        $views = $this->getMockBuilder('\Tripod\Mongo\Composites\Views')
            ->setMethods(['getImpactedSubjects'])
            ->disableOriginalConstructor()
            ->getMock();

        $tripod->expects($this->once())->method('getComposite')
            ->with(OP_VIEWS)
            ->will($this->returnValue($views));

        $views->expects($this->once())->method('getImpactedSubjects')->will($this->returnValue($impactedSubjects));

        /** @var \Tripod\Mongo\Jobs\DiscoverImpactedSubjects|PHPUnit_Framework_MockObject_MockObject $discoverJob */
        $discoverJob = $this->getMockBuilder('\Tripod\Mongo\Jobs\DiscoverImpactedSubjects')
            ->setMethods(['getTripod', 'getApplyOperation'])
            ->getMock();

        /** @var \Tripod\Mongo\Jobs\ApplyOperation|PHPUnit_Framework_MockObject_MockObject $applyJob */
        $applyJob = $this->getMockBuilder('\Tripod\Mongo\Jobs\ApplyOperation')
            ->setMethods(['submitJob'])
            ->setMockClassName('ApplyOperation_TestConfigGenerator')
            ->getMock();
        $discoverJob->args = $jobArgs;
        $discoverJob->job = (object) ['payload' => ['id' => uniqid()]];
        $discoverJob->expects($this->once())->method('getTripod')->will($this->returnValue($tripod));
        $discoverJob->expects($this->once())->method('getApplyOperation')->will($this->returnValue($applyJob));
        $configInstance = \Tripod\Config::getInstance();
        $applyJob->expects($this->once())->method('submitJob')
            ->with(
                $configInstance::getApplyQueueName(),
                'ApplyOperation_TestConfigGenerator',
                [
                    ApplyOperation::SUBJECTS_KEY => [
                        [
                            'resourceId' => [
                                _ID_RESOURCE => 'http://example.com/1',
                                _ID_CONTEXT => 'http://talisaspire.com/'
                            ],
                            'operation' => OP_VIEWS,
                            'specTypes' => ['v_resource_full'],
                            'storeName' => 'tripod_php_testing',
                            'podName' => 'CBD_testing'
                        ]
                    ],
                    JobBase::TRIPOD_CONFIG_GENERATOR => $this->config
                ]
            );
        $discoverJob->setUp();
        $discoverJob->perform();
    }
}
