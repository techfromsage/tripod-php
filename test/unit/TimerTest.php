<?php

use PHPUnit\Framework\TestCase;
use Tripod\Timer;

class TimerTest extends TestCase
{
    protected function setUp(): void
    {
        printf(" %s->%s\n", get_class($this), $this->getName());
    }

    /**  START: result() tests */
    public function testResultWhenStartTimeNotSet()
    {
        $timer = new Timer();
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Timer: start method not called !');
        $timer->result();
    }

    public function testResultWhenEndTimeNotSet()
    {
        $timer = new Timer();
        $timer->start();
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Timer: stop method not called !');
        $timer->result();
    }

    public function testResultGetTimeInMilliSeconds()
    {
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we will get 0 as a result.
        $timer->stop();
        $status = ($timer->result() >= 1000) ? true : false;
        $this->assertTrue($status);
    }
    /**  END: result() tests */

    /**  START: microResult() tests */
    public function testMicroResultWhenStartTimeNotSet()
    {
        $timer = new Timer();
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Timer: start method not called !');
        $timer->result();
    }

    public function testMicroResultWhenEndTimeNotSet()
    {
        $timer = new Timer();
        $timer->start();
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Timer: stop method not called !');
        $timer->result();
    }

    public function testMicroResultGetTimeInMilliSeconds()
    {
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we might get 0 as a result.
        $timer->stop();
        $status = ($timer->microResult() >= 1000000) ? true : false;
        $this->assertTrue($status);
    }
    /**  END: microResult() tests */
}
