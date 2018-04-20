<?php

require_once 'MongoTripodTestBase.php';
require_once 'TestConfigGenerator.php';

class ConfigGeneratorTest extends MongoTripodTestBase
{
    public function testCreateFromConfig()
    {
        \Tripod\Config::setConfig(
            [
                'class' => 'TestConfigGenerator',
                'filename' => dirname(__FILE__) . '/data/config.json'
            ]
        );
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
        $config = [
            'class' => 'TestConfigGenerator',
            'filename' => dirname(__FILE__) . '/data/config.json'
        ];
        \Tripod\Config::setConfig($config);

        /** @var TestConfigGenerator $instance */
        $instance = \Tripod\Config::getInstance();
        $this->assertEquals($config, $instance->serialize());
    }
}
