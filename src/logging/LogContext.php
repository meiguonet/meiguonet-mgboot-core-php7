<?php

namespace mgboot\logging;

use mgboot\common\util\ArrayUtils;
use Psr\Log\LoggerInterface;

final class LogContext
{
    /**
     * @var array
     */
    private static $loggers = [];

    /**
     * @var LoggerInterface|null
     */
    private static $runtimeLogger = null;

    /**
     * @var LoggerInterface|null
     */
    private static $requestLogLogger = null;

    /**
     * @var LoggerInterface|null
     */
    private static $executeTimeLogLogger = null;

    private function __construct()
    {
    }

    /**
     * @param Logger|array $arg0
     */
    public static function withLogger($arg0): void
    {
        $logger = null;

        if ($arg0 instanceof Logger) {
            $logger = $arg0;
        } else if (is_array($arg0) && !empty($arg0) && ArrayUtils::isAssocArray($arg0)) {
            $logger = Logger::create($arg0);
        }

        if (!($logger instanceof Logger) || $logger->isNoop() || $logger->getChannel() === '') {
            return;
        }

        $idx = -1;

        foreach (self::$loggers as $i => $lg) {
            if (!($lg instanceof Logger)) {
                continue;
            }

            if ($lg->getChannel() === $logger->getChannel()) {
                $idx = $i;
                break;
            }
        }

        if ($idx >= 0) {
            self::$loggers[$idx] = $logger;
        } else {
            self::$loggers[] = $logger;
        }
    }

    public static function getLogger(string $name): LoggerInterface
    {
        $logger = null;

        foreach (self::$loggers as $lg) {
            if (!($lg instanceof Logger)) {
                continue;
            }

            if ($lg->getChannel() === $name) {
                $logger = $lg;
                break;
            }
        }

        return $logger instanceof Logger ? $logger : Logger::create(['noop' => true]);
    }

    public static function withRuntimeLogger(string $name = 'runtime'): void
    {
        self::$runtimeLogger = self::getLogger($name);
    }

    public static function getRuntimeLogger(): LoggerInterface
    {
        $logger = self::$runtimeLogger;
        return $logger instanceof Logger ? $logger : Logger::create(['noop' => true]);
    }

    public static function withRequestLogLogger(string $name = 'request'): void
    {
        self::$requestLogLogger = self::getLogger($name);
    }

    public static function getRequestLogLogger(): ?LoggerInterface
    {
        $logger = self::$requestLogLogger;
        return $logger instanceof Logger && !$logger->isNoop() ? $logger : null;
    }

    public static function requestLogEnabled(): bool
    {
        return self::getRequestLogLogger() !== null;
    }

    public static function withExecuteTimeLogLogger(string $name = 'request'): void
    {
        self::$executeTimeLogLogger = self::getLogger($name);
    }

    public static function getExecuteTimeLogLogger(): ?LoggerInterface
    {
        $logger = self::$executeTimeLogLogger;
        return $logger instanceof Logger && !$logger->isNoop() ? $logger : null;
    }

    public static function executeTimeLogEnabled(): bool
    {
        return self::getExecuteTimeLogLogger() !== null;
    }
}
