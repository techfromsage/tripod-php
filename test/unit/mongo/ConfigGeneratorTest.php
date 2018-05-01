<?php

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
                    'resetOriginalReadPreference'
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
                'statsConfig' => []
            ]);

        $tripod->saveChanges(
            $originalGraph,
            $newGraph
        );
    }
}
