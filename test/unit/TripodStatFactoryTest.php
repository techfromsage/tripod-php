<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Tripod\Exceptions\ConfigException;
use Tripod\NoStat;
use Tripod\TripodStatFactory;

class TripodStatFactoryTest extends TestCase
{
    public function testCreateDefaultsToNoStat(): void
    {
        $this->assertInstanceOf(NoStat::class, TripodStatFactory::create());
        $this->assertInstanceOf(NoStat::class, TripodStatFactory::create(['class' => 'ThisClassDoesNotExist']));
    }

    public function testCreateThrowsWhenClassHasNoFactoryMethod(): void
    {
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('stdClass does not provide a callable createFromConfig() method');

        TripodStatFactory::create(['class' => 'stdClass']);
    }

    public function testCreateThrowsWhenFactoryReturnsWrongType(): void
    {
        $this->expectException(ConfigException::class);
        $this->expectExceptionMessage('BadStatFactoryFixture::createFromConfig() did not return an ITripodStat');

        TripodStatFactory::create(['class' => 'BadStatFactoryFixture']);
    }
}

class BadStatFactoryFixture
{
    public static function createFromConfig(array $config): string
    {
        return 'not a stat instance';
    }
}
