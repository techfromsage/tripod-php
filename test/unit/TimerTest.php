<?php
set_include_path(
    get_include_path()
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__)))
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/lib'
        . PATH_SEPARATOR . dirname(dirname(dirname(__FILE__))).'/src');

require_once 'src/classes/Timer.class.php';

use \Tripod\Timer;

class TimerTest extends PHPUnit_Framework_TestCase
{
    protected function setUp()
    {
        $className = get_class($this);
        $testName = $this->getName();
        echo "\nTest: {$className}->{$testName}\n";
    }

    /**  START: result() tests */
    public function testResultWhenStartTimeNotSet(){
        $timer = new Timer();
        $this->setExpectedException('Exception', 'Timer: start method not called !');
        $timer->result();
    }

    public function testResultWhenEndTimeNotSet(){
        $timer = new Timer();
        $timer->start();
        $this->setExpectedException('Exception', 'Timer: stop method not called !');
        $timer->result();
    }

    public function testResultGetTimeInMilliSeconds(){
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we will get 0 as a result.
        $timer->stop();
        $status = ($timer->result() >=1000)? true: false;
        $this->assertTrue($status);
    }
    /**  END: result() tests */

    /**  START: microResult() tests */
    public function testMicroResultWhenStartTimeNotSet(){
        $timer = new Timer();
        $this->setExpectedException('Exception', 'Timer: start method not called !');
        $timer->result();
    }

    public function testMicroResultWhenEndTimeNotSet(){
        $timer = new Timer();
        $timer->start();
        $this->setExpectedException('Exception', 'Timer: stop method not called !');
        $timer->result();
    }

    public function testMicroResultGetTimeInMilliSeconds(){
        $timer = new Timer();
        $timer->start();
        sleep(1); // Let's pause for one seconds otherwise we might get 0 as a result.
        $timer->stop();
        $status = ($timer->microResult() >=1000000)? true: false;
        $this->assertTrue($status);
    }
    /**  END: microResult() tests */
}