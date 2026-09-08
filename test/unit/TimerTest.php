<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Tripod\Timer;

class TimerTest extends TestCase
{
    /**  START: result() tests */
    public function testResultWhenStartTimeNotSet(): void
    {
        $timer = new Timer();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Timer: start method not called !');
        $timer->result();
    }

    public function testResultWhenEndTimeNotSet(): void
    {
        $timer = new Timer();
        $timer->start();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Timer: stop method not called !');
        $timer->result();
    }

    public function testResultGetTimeInMilliSeconds(): void
    {
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we will get 0 as a result.
        $timer->stop();
        $status = $timer->result() >= 1000;
        $this->assertTrue($status);
    }

    /**  END: result() tests */

    /**  START: microResult() tests */
    public function testMicroResultWhenStartTimeNotSet(): void
    {
        $timer = new Timer();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Timer: start method not called !');
        $timer->microResult();
    }

    public function testMicroResultWhenEndTimeNotSet(): void
    {
        $timer = new Timer();
        $timer->start();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Timer: stop method not called !');
        $timer->microResult();
    }

    public function testMicroResultGetTimeInMilliSeconds(): void
    {
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we might get 0 as a result.
        $timer->stop();
        $status = $timer->microResult() >= 1000000;
        $this->assertTrue($status);
    }

    // END: microResult() tests
}
