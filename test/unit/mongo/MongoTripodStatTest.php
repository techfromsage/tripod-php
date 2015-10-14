<?php
require_once 'MongoTripodTestBase.php';

/**
 * Class MongoTripodStatTest
 */
class MongoTripodStatTest extends MongoTripodTestBase
{

    public function testStatFactory()
    {
        $statConfig = $this->getStatsDConfig();

        /** @var \Tripod\StatsD $stat */
        $stat = \Tripod\TripodStatFactory::create($statConfig);
        $this->assertInstanceOf('\Tripod\StatsD', $stat);
        $this->assertEquals('example.com', $stat->getHost());
        $this->assertEquals(1234, $stat->getPort());
        $this->assertEquals('somePrefix', $stat->getPrefix());

        $noStat = \Tripod\TripodStatFactory::create();
        $this->assertInstanceOf('\Tripod\Mongo\NoStat', $noStat);
    }

    public function testStatsDSettersAndGetters()
    {
        $stat = \Tripod\StatsD::createFromConfig($this->getStatsDConfig());

        $this->assertInstanceOf('\Tripod\StatsD', $stat);
        $this->assertEquals('example.com', $stat->getHost());
        $this->assertEquals(1234, $stat->getPort());
        $this->assertEquals('somePrefix', $stat->getPrefix());

        $this->assertEquals($this->getStatsDConfig(), $stat->getConfig());

        $stat = new \Tripod\StatsD('foo.bar', 9876);
        $this->assertEquals('foo.bar', $stat->getHost());
        $this->assertEquals(9876, $stat->getPort());
        $this->assertEquals('', $stat->getPrefix());
        $this->assertEquals(array('class'=>'Tripod\StatsD', 'config'=>array('host'=>'foo.bar','port'=>9876,'prefix'=>'')), $stat->getConfig());

        $stat->setHost('bar.baz');
        $this->assertEquals('bar.baz', $stat->getHost());

        $stat->setPort(4567);
        $this->assertEquals(4567, $stat->getPort());
        $stat->setPrefix('FOO_BAR');
        $this->assertEquals('FOO_BAR', $stat->getPrefix());

        $this->assertEquals(array('class'=>'Tripod\StatsD', 'config'=>array('host'=>'bar.baz','port'=>4567,'prefix'=>'FOO_BAR')), $stat->getConfig());
    }

    public function testStatsDIncrementNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(STAT_CLASS.'.FOO.BAR'=>"1|c"),
                1
            );


        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPivotValueNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);

        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    STAT_CLASS.'.'.STAT_PIVOT_FIELD.'.wibble.FOO.BAR'=>'1|c',
                    STAT_CLASS.'.FOO.BAR'=>"1|c"
                ),
                1
            );

        $stat->setPivotValue('wibble');

        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array('somePrefix.' . STAT_CLASS. '.FOO.BAR'=>"1|c"),
                1
            );


        $stat->increment('FOO.BAR');
    }

    public function testStatsDIncrementWithPivotValueAndPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    'somePrefix.' . STAT_CLASS . '.' . STAT_PIVOT_FIELD . '.wibble.FOO.BAR'=>"5|c",
                    'somePrefix.' . STAT_CLASS.'.FOO.BAR'=>"5|c"
                ),
                1
            );

        $stat->setPivotValue('wibble');
        $stat->increment('FOO.BAR', 5);
    }

    public function testStatsDTimerNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(STAT_CLASS . '.FOO.BAR'=>array("1|c","1234|ms")),
                1
            );


        $stat->timer('FOO.BAR', 1234);
    }

    public function testStatsDTimerWithPivotValueNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    STAT_CLASS . '.' . STAT_PIVOT_FIELD . '.wibble.FOO.BAR'=>array("1|c","1234|ms"),
                    STAT_CLASS . '.FOO.BAR'=>array("1|c","1234|ms")
                ),
                1
            );

        $stat->setPivotValue('wibble');
        $stat->timer('FOO.BAR', 1234);
    }
    public function testStatsDTimerWithPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array('somePrefix.' . STAT_CLASS. '.FOO.BAR'=>array("1|c","4567|ms")),
                1
            );


        $stat->timer('FOO.BAR',4567);
    }

    public function testStatsDTimerWithPrefixAndPivotValue()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    'somePrefix.' . STAT_CLASS . '.' . STAT_PIVOT_FIELD . '.wibble.FOO.BAR'=>array("1|c","4567|ms"),
                    'somePrefix.' . STAT_CLASS . '.FOO.BAR'=>array("1|c","4567|ms")
                ),
                1
            );

        $stat->setPivotValue('wibble');
        $stat->timer('FOO.BAR',4567);
    }

    public function testStatsDGaugeNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(STAT_CLASS.'.FOO.BAR'=>"xyz|g"),
                1
            );


        $stat->gauge('FOO.BAR', 'xyz');
    }

    public function testStatsDGaugeWithPivotValueNoPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    STAT_CLASS. '.' . STAT_PIVOT_FIELD .'.wibble.FOO.BAR'=>"xyz|g",
                    STAT_CLASS.'.FOO.BAR'=>"xyz|g"
                ),
                1
            );
        $stat->setPivotValue('wibble');

        $stat->gauge('FOO.BAR', 'xyz');
    }

    public function testStatsDGaugeWithPrefix()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array('somePrefix.' . STAT_CLASS . '.FOO.BAR'=>"abc|g"),
                1
            );


        $stat->gauge('FOO.BAR', 'abc');
    }

    public function testStatsDGaugeWithPrefixAndPivotValue()
    {
        $statConfig = $this->getStatsDConfig();

        $stat = $this->getMockStat($statConfig['config']['host'], $statConfig['config']['port'], $statConfig['config']['prefix']);
        $stat->expects($this->once())
            ->method('send')
            ->with(
                array(
                    'somePrefix.' . STAT_CLASS . '.' . STAT_PIVOT_FIELD . '.wibble.FOO.BAR'=>"abc|g",
                    'somePrefix.' . STAT_CLASS . '.FOO.BAR'=>"abc|g"
                ),
                1
            );

        $stat->setPivotValue('wibble');
        $stat->gauge('FOO.BAR', 'abc');
    }

    public function testPrefixCannotStartWithDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid prefix supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567, '.some_prefix');
    }

    public function testPrefixCannotEndWithDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid prefix supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567, 'some_prefix.');
    }

    public function testPrefixCannotContainConsecutiveDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid prefix supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567, 'some..prefix');
    }

    public function testPivotValueCannotStartWithDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid pivot value supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567);
        $stat->setPivotValue('.someValue');
    }

    public function testPivotValueCannotEndWithDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid pivot value supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567);
        $stat->setPivotValue('someValue.');
    }

    public function testPivotValueCannotContainConsecutiveDot()
    {
        $this->setExpectedException('InvalidArgumentException', 'Invalid pivot value supplied');

        $stat = new \Tripod\StatsD('foo.bar', 4567);
        $stat->setPivotValue('some..value');
    }
}