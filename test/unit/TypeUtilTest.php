<?php

declare(strict_types=1);

use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Tripod\Mongo\DriverBase;
use Tripod\TypeUtil;

class TypeUtilTest extends TestCase
{
    private TestHandler $log;

    public function setUp(): void
    {
        parent::setUp();
        $this->log = new TestHandler();
        $logger = new Logger('unittest');
        $logger->pushHandler($this->log);
        DriverBase::$logger = $logger;
    }

    /**
     * @testWith [1234, "1234"]
     *           [3.14, "3.14"]
     *           [true, "1"]
     *
     * @param mixed $value
     * @param mixed $expected
     */
    public function testEnsureArgIsString($value, $expected): void
    {
        $result = TypeUtil::ensureArgIsString(1, $value);
        $this->assertSame($expected, $result);
        $this->assertTrue($this->log->hasWarningThatContains(
            'TypeError: Argument 1 passed to ' . __CLASS__ . '::' . __FUNCTION__
                . '() must be of the type string, ' . gettype($value)
                . ' given (' . var_export($value, true) . ')'
        ));
    }

    public function testEnsureArgIsStringNotNull(): void
    {
        $result = TypeUtil::ensureArgIsString(1, null);
        $this->assertSame('', $result);
        $this->assertTrue($this->log->hasWarningThatContains(
            'TypeError: Argument 1 passed to ' . __CLASS__ . '::' . __FUNCTION__
                . '() must be of the type string, NULL given'
        ));
    }

    /**
     * @testWith [1234, "1234"]
     *           [3.14, "3.14"]
     *           [true, "1"]
     *
     * @param mixed $value
     * @param mixed $expected
     */
    public function testEnsureArgIsStringOrNull($value, $expected): void
    {
        $result = TypeUtil::ensureArgIsStringIsOrNull(1, $value);
        $this->assertIsString($result);
        $this->assertSame($expected, $result);
        $this->assertTrue($this->log->hasWarningThatContains(
            'TypeError: Argument 1 passed to ' . __CLASS__ . '::' . __FUNCTION__
                . '() must be of the type string or null, ' . gettype($value)
                . ' given (' . var_export($value, true) . ')'
        ));
    }
}
