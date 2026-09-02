<?php

declare(strict_types=1);

use Tripod\Config;
use Tripod\Mongo\DateUtil;

class DateUtilTest extends MongoTripodTestBase
{
    public function testGetMongoDateWithNoParam(): void
    {
        $config = Config::getInstance();
        $updatedAt = (new DateUtil())->getMongoDate();

        $_id = [
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/',
        ];
        $doc = [
            '_id' => $_id,
            'dct:title' => ['l' => 'etag'],
            '_version' => 0,
            '_cts' => $updatedAt,
            '_uts' => $updatedAt,
        ];
        $config->getCollectionForCBD(
            'tripod_php_testing',
            'CBD_testing'
        )->insertOne($doc, ['w' => 1]);

        $date = DateUtil::getMongoDate();

        $this->assertSame(13, strlen($date->__toString()));
    }

    public function testGetMongoDateWithParam(): void
    {
        $config = Config::getInstance();
        $updatedAt = (new DateUtil())->getMongoDate();

        $_id = [
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/',
        ];
        $doc = [
            '_id' => $_id,
            'dct:title' => ['l' => 'etag'],
            '_version' => 0,
            '_cts' => $updatedAt,
            '_uts' => $updatedAt,
        ];
        $config->getCollectionForCBD(
            'tripod_php_testing',
            'CBD_testing'
        )->insertOne($doc, ['w' => 1]);

        $time = floor(microtime(true) * 1000);
        $date = DateUtil::getMongoDate($time);

        $this->assertSame(13, strlen($date->__toString()));
        $this->assertEquals($time, $date->__toString());
    }
}
