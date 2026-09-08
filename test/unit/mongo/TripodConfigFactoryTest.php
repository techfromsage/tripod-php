<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\Exceptions\ConfigException;
use Tripod\Mongo\Config as MongoConfig;
use Tripod\TripodConfigFactory;

class TripodConfigFactoryTest extends MongoTripodTestBase
{
    public function testCreateFromPlainConfigArrayReturnsMongoConfig(): void
    {
        $config = $this->decodeJsonFile(__DIR__ . '/data/config.json');

        $instance = TripodConfigFactory::create($config);

        $this->assertInstanceOf(MongoConfig::class, $instance);
        $this->assertSame($config, Config::getConfig());
    }

    public function testCreateFromSerializedConfigGenerator(): void
    {
        $instance = TripodConfigFactory::create([
            'class' => 'TestConfigGenerator',
            'filename' => __DIR__ . '/data/config.json',
        ]);

        $this->assertInstanceOf(TestConfigGenerator::class, $instance);
        $this->assertContains('CBD_testing', $instance->getPods('tripod_php_testing'));
    }

    public function testCreateThrowsWhenClassHasNoDeserializeMethod(): void
    {
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('stdClass does not provide a callable deserialize() method');

        TripodConfigFactory::create(['class' => 'stdClass']);
    }

    public function testCreateThrowsWhenDeserializeReturnsWrongType(): void
    {
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('BadConfigDeserializerFixture::deserialize() did not return an IConfigInstance');

        TripodConfigFactory::create(['class' => 'BadConfigDeserializerFixture']);
    }
}

class BadConfigDeserializerFixture
{
    public static function deserialize(array $config): string
    {
        return 'not a config instance';
    }
}
