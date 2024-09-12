<?php

class DateUtilTest extends MongoTripodTestBase
{
    public function testGetMongoDateWithNoParam()
    {
        $config = Tripod\Config::getInstance();
        $updatedAt = (new Tripod\Mongo\DateUtil())->getMongoDate();

        $_id = [
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/'];
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

        $date = Tripod\Mongo\DateUtil::getMongoDate();

        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $date);
        $this->assertEquals(13, strlen($date->__toString()));
    }

    public function testGetMongoDateWithParam()
    {
        $config = Tripod\Config::getInstance();
        $updatedAt = (new Tripod\Mongo\DateUtil())->getMongoDate();

        $_id = [
            'r' => 'http://talisaspire.com/resources/testEtag' . microtime(false),
            'c' => 'http://talisaspire.com/'];
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
        $date = Tripod\Mongo\DateUtil::getMongoDate($time);

        $this->assertInstanceOf(MongoDB\BSON\UTCDateTime::class, $date);
        $this->assertEquals(13, strlen($date->__toString()));
        $this->assertEquals($time, $date->__toString());
    }
}
