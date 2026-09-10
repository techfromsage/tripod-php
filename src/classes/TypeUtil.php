<?php

declare(strict_types=0);

namespace Tripod;

use Tripod\Mongo\DriverBase;

class TypeUtil
{
    /**
     * @param mixed $value
     */
    public static function ensureArgIsString(int $argIndex, $value): string
    {
        if (!is_string($value)) {
            self::logArgTypeError('must be of the type string', $argIndex, $value);
        }

        return self::stringify($value);
    }

    /**
     * @param mixed $value
     */
    public static function ensureArgIsStringIsOrNull(int $argIndex, $value): ?string
    {
        if (!is_string($value) && !is_null($value)) {
            self::logArgTypeError('must be of the type string or null', $argIndex, $value);
        }

        return is_null($value) ? null : self::stringify($value);
    }

    /**
     * @param mixed $value
     */
    private static function stringify($value): string
    {
        return is_scalar($value) ? (string) $value : '';
    }

    /**
     * @param mixed $value
     */
    private static function logArgTypeError(string $message, int $argIndex, $value): void
    {
        $error = self::makeArgTypeError($message, $argIndex, $value);
        DriverBase::getLogger()->warning((string) $error);
    }

    /**
     * @param mixed $value
     */
    private static function makeArgTypeError(string $message, int $argIndex, $value): \TypeError
    {
        $caller = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 4)[3] ?? [];
        $message = sprintf(
            'Argument %d passed to %s%s%s() %s, %s given',
            $argIndex,
            $caller['class'] ?? '',
            isset($caller['class']) ? '::' : '',
            $caller['function'] ?? 'function',
            $message,
            gettype($value)
        );

        if ($value === null) {
            return new \TypeError($message);
        }

        return new \TypeError(sprintf(
            '%s (%s)',
            $message,
            var_export($value, true)
        ));
    }
}
