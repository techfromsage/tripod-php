<?php

declare(strict_types=1);

use MongoDB\BSON\ObjectId;
use Tripod\Mongo\JobGroup;

class JobGroupTest extends MongoTripodTestBase
{
    public function testConstructorGeneratesIdWhenNoneSupplied(): void
    {
        $jobGroup = new JobGroup('tripod_php_testing');
        $this->assertInstanceOf(ObjectId::class, $jobGroup->getId());
    }

    public function testConstructorAcceptsObjectId(): void
    {
        $id = new ObjectId();
        $jobGroup = new JobGroup('tripod_php_testing', $id);
        $this->assertSame($id, $jobGroup->getId());
    }

    public function testConstructorAcceptsStringId(): void
    {
        $id = new ObjectId();
        $jobGroup = new JobGroup('tripod_php_testing', (string) $id);
        $this->assertEquals($id, $jobGroup->getId());
    }

    public function testSetJobCountAndIncrementJobCount(): void
    {
        $jobGroup = new JobGroup('tripod_php_testing');
        $jobGroup->setJobCount(10);

        $this->assertSame(11, $jobGroup->incrementJobCount());
        $this->assertSame(16, $jobGroup->incrementJobCount(5));
        $this->assertSame(12, $jobGroup->incrementJobCount(-4));
    }

    public function testIncrementJobCountUpsertsWhenNoCountSet(): void
    {
        $jobGroup = new JobGroup('tripod_php_testing');
        $this->assertSame(3, $jobGroup->incrementJobCount(3));
    }
}
