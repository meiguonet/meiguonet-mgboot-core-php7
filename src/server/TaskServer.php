<?php

namespace mgboot\server;

use Carbon\Carbon;
use Cron\CronExpression;
use DateTime;
use DateTimeZone;
use mgboot\annotation\Scheduled;
use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\FileUtils;
use mgboot\common\util\JsonUtils;
use mgboot\common\util\ReflectUtils;
use mgboot\common\util\StringUtils;
use mgboot\common\util\TokenizeUtils;
use mgboot\dal\redis\RedisCmd;
use mgboot\task\TaskPublisher;
use ReflectionClass;
use Throwable;
use Workerman\Timer;
use Workerman\Worker;

final class TaskServer
{
    /**
     * @var string
     */
    private static $cronTaskSourceDir = 'classpath:app/cron';

    /**
     * @var array
     */
    private static $cronTasks = [];

    /**
     * @var callable|null
     */
    private static $cronTaskExecutor = null;

    /**
     * @var callable|null
     */
    private static $taskExecutor = null;

    public static function start(bool $inSwooleMode = false): bool
    {
        if ($inSwooleMode) {
            $server = Swoole::getServer();

            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            if (!($server instanceof \Swoole\Server)) {
                return false;
            }

            self::buildCronTasks(true);
            self::runInSwooleMode($server);
            return true;
        }

        self::buildCronTasks();
        $cronTaskExecutor = self::$cronTaskExecutor;
        $taskExecutor = self::$taskExecutor;
        $worker = new Worker();

        try {
            $worker->count = 1;
        } catch (Throwable $ex) {
        }

        $worker->onWorkerStart = function () use ($cronTaskExecutor, $taskExecutor) {
            if (is_callable($cronTaskExecutor)) {
                foreach (TaskServer::getCronTasks() as $item) {
                    if (!is_int($item['interval'])) {
                        continue;
                    }

                    $taskClass = $item['taskClass'];

                    Timer::add(floatval($item['interval']), function () use ($cronTaskExecutor, $taskClass) {
                        call_user_func($cronTaskExecutor, $taskClass);
                    });
                }

                Timer::add(1.0, function () use ($cronTaskExecutor) {
                    $now = new DateTime();

                    foreach (TaskServer::getCronTasks() as $i => $item) {
                        if (is_int($item['interval'])) {
                            continue;
                        }

                        $schedules = $item['schedules'];

                        if (!is_array($schedules)) {
                            try {
                                $cron = new CronExpression($item['expr']);
                            } catch (Throwable $ex) {
                                $cron = null;
                            }

                            if (!($cron instanceof CronExpression)) {
                                continue;
                            }

                            $schedules = array_map(function (DateTime $it) {
                                return $it->getTimestamp();
                            }, $cron->getMultipleRunDates(100, $now));
                        }

                        if (empty($schedules) || $now->getTimestamp() < $schedules[0]) {
                            continue;
                        }

                        array_shift($schedules);
                        TaskServer::updateCronTaskSchedules($i, $schedules);
                        call_user_func($cronTaskExecutor, $item['taskClass']);
                    }
                });
            }

            if (!is_callable($taskExecutor)) {
                return;
            }

            Timer::add(2.0, function () use ($taskExecutor) {
                $cacheKey = TaskPublisher::normalQueueCacheKey();
                $n1 = 20;
                $n2 = 1;
                $payloads = [];

                while (true) {
                    if ($n2 > $n1) {
                        break;
                    }

                    try {
                        $payload = RedisCmd::lPop($cacheKey);
                    } catch (Throwable $ex) {
                        $payload = null;
                    }

                    if (!is_string($payload) || $payload === '') {
                        break;
                    }

                    $n2++;
                    $payloads[] = $payload;
                }

                foreach ($payloads as $payload) {
                    call_user_func($taskExecutor, $payload);
                }
            });

            Timer::add(2.0, function () use ($taskExecutor) {
                $cacheKey = TaskPublisher::delayableQueueCacheKey();
                $now = Carbon::now(new DateTimeZone('Asia/Shanghai'))->timestamp;

                try {
                    $entries = RedisCmd::zRangeByScore($cacheKey, $now - 3600, $now + 30);
                } catch (Throwable $ex) {
                    $entries = null;
                }

                if (!is_array($entries) || empty($entries)) {
                    return;
                }

                $payloads = [];
                $itemsToRemove = [];

                foreach ($entries as $payload) {
                    if (!is_string($payload) || $payload === '') {
                        continue;
                    }

                    $map1 = JsonUtils::mapFrom($payload);

                    if (!is_array($map1) || empty($map1)) {
                        $itemsToRemove[] = $payload;
                        continue;
                    }

                    $runAt = Cast::toInt($map1['runAt']);

                    if ($runAt > $now) {
                        continue;
                    }

                    $itemsToRemove[] = $payload;

                    if ($now - $runAt <= 5) {
                        $payloads[] = $payload;
                    }
                }

                if (!empty($itemsToRemove)) {
                    try {
                        RedisCmd::zRem($cacheKey, ...$itemsToRemove);
                    } catch (Throwable $ex) {
                    }
                }

                foreach ($payloads as $payload) {
                    call_user_func($taskExecutor, $payload);
                }
            });
        };

        Worker::runAll();
        return true;
    }

    public static function scanCronTaskIn(string $dir): void
    {
        self::$cronTaskSourceDir = $dir;
    }

    public static function buildCronTasks(bool $inSwooleMode = false): void
    {
        $dir = FileUtils::getRealpath(self::$cronTaskSourceDir);
        $dir = str_replace("\\", '/', $dir);

        if ($dir !== '/') {
            $dir = rtrim($dir, '/');
        }

        if (!is_dir($dir)) {
            return;
        }

        $files = [];
        FileUtils::scanFiles($dir, $files);
        $items = [];
        $now = new DateTime();

        foreach ($files as $fpath) {
            if (!preg_match('/\.php$/', $fpath)) {
                continue;
            }

            try {
                $tokens = token_get_all(file_get_contents($fpath));
                $className = TokenizeUtils::getQualifiedClassName($tokens);
                $clazz = new ReflectionClass($className);
            } catch (Throwable $ex) {
                $className = '';
                $clazz = null;
            }

            if (empty($className) || !($clazz instanceof ReflectionClass)) {
                continue;
            }

            $anno = ReflectUtils::getClassAnnotation($clazz, Scheduled::class);

            if (!($anno instanceof Scheduled) || $anno->isDisabled()) {
                continue;
            }

            $expr = $anno->getValue();

            if (empty($expr)) {
                continue;
            }

            if (StringUtils::startsWith($expr, '@every')) {
                $n1 = Cast::toDuration(StringUtils::substringAfter($expr, ' '));

                if ($n1 > 0) {
                    $items[] = [
                        'interval' => $n1,
                        'taskClass' => str_replace("\\", '/', $className)
                    ];
                }

                continue;
            }

            try {
                $cron = new CronExpression($expr);
            } catch (Throwable $ex) {
                $cron = null;
            }

            if (!($cron instanceof CronExpression)) {
                continue;
            }

            $schedules = array_map(function (DateTime $it) {
                return $it->getTimestamp();
            }, $cron->getMultipleRunDates(100, $now));

            $items[] = [
                'taskClass' => str_replace("\\", '/', $className),
                'expr' => $expr,
                'schedules' => $schedules
            ];
        }

        if (empty($items)) {
            return;
        }

        if ($inSwooleMode) {
            $items = JsonUtils::toJson($items);
            SwooleTable::setValue(SwooleTable::cronTableName(), 'cronTasks', compact('items'));
            return;
        }

        self::$cronTasks = $items;
    }

    public static function getCronTasks(bool $inSwooleMode = false): array
    {
        if (!$inSwooleMode) {
            return self::$cronTasks;
        }

        $data = SwooleTable::getValue(SwooleTable::cronTableName(), 'cronTasks');

        if (!is_array($data) || !is_string($data['items'])) {
            return [];
        }

        return JsonUtils::arrayFrom($data['items']);
    }

    public static function updateCronTaskSchedules(int $idx, array $schedules, bool $inSwooleMode = false): void
    {
        if (!$inSwooleMode) {
            self::$cronTasks[$idx]['schedules'] = $schedules;
            return;
        }

        $data = SwooleTable::getValue(SwooleTable::cronTableName(), 'cronTasks');

        if (!is_array($data) || !is_string($data['items'])) {
            return;
        }

        $items = JsonUtils::arrayFrom($data['items']);

        if ($idx > count($items) - 1) {
            return;
        }

        $items[$idx]['schedules'] = $schedules;
        $items = JsonUtils::toJson($items);
        SwooleTable::setValue(SwooleTable::cronTableName(), 'cronTasks', compact('items'));
    }

    public static function withCronTaskExecutor(callable $callback): void
    {
        self::$cronTaskExecutor = $callback;
    }

    public static function withTaskExecutor(callable $callback): void
    {
        self::$taskExecutor = $callback;
    }

    private static function runInSwooleMode($server): void
    {
        $cronTaskExecutor = self::$cronTaskExecutor;

        if (is_callable($cronTaskExecutor)) {
            foreach (self::getCronTasks(true) as $item) {
                if (!is_int($item['interval'])) {
                    continue;
                }

                $taskClass = $item['taskClass'];

                Swoole::timerTick($item['interval'] * 1000, function () use ($server, $cronTaskExecutor, $taskClass) {
                    call_user_func($cronTaskExecutor, $server, $taskClass);
                });
            }

            Swoole::timerTick(1000, function () use ($server, $cronTaskExecutor) {
                $now = new DateTime();

                foreach (TaskServer::getCronTasks(true) as $i => $item) {
                    if (is_int($item['interval'])) {
                        continue;
                    }

                    $schedules = $item['schedules'];

                    if (!is_array($schedules)) {
                        try {
                            $cron = new CronExpression($item['expr']);
                        } catch (Throwable $ex) {
                            $cron = null;
                        }

                        if (!($cron instanceof CronExpression)) {
                            continue;
                        }

                        $schedules = array_map(function (DateTime $it) {
                            return $it->getTimestamp();
                        }, $cron->getMultipleRunDates(100, $now));
                    }

                    if (empty($schedules) || $now->getTimestamp() < $schedules[0]) {
                        continue;
                    }

                    array_shift($schedules);
                    TaskServer::updateCronTaskSchedules($i, $schedules, true);
                    call_user_func($cronTaskExecutor, $server, $item['taskClass']);
                }
            });
        }

        $taskExecutor = self::$taskExecutor;

        if (!is_callable($taskExecutor)) {
            return;
        }

        Swoole::timerTick(2000, function () use ($server, $taskExecutor) {
            $cacheKey = TaskPublisher::normalQueueCacheKey();
            $n1 = 20;
            $n2 = 1;
            $payloads = [];

            while (true) {
                if ($n2 > $n1) {
                    break;
                }

                try {
                    $payload = RedisCmd::lPop($cacheKey);
                } catch (Throwable $ex) {
                    $payload = null;
                }

                if (!is_string($payload) || $payload === '') {
                    break;
                }

                $n2++;
                $payloads[] = $payload;
            }

            foreach ($payloads as $payload) {
                call_user_func($taskExecutor, $server, $payload);
            }
        });

        Swoole::timerTick(2000, function () use ($server, $taskExecutor) {
            $now = Carbon::now(new DateTimeZone('Asia/Shanghai'))->timestamp;
            $cacheKey = TaskPublisher::delayableQueueCacheKey();

            try {
                $entries = RedisCmd::zRangeByScore($cacheKey, $now - 3600, $now + 30);
            } catch (Throwable $ex) {
                $entries = null;
            }

            if (!is_array($entries) || empty($entries)) {
                return;
            }

            $payloads = [];
            $itemsToRemove = [];

            foreach ($entries as $payload) {
                if (!is_string($payload) || $payload === '') {
                    continue;
                }

                $map1 = JsonUtils::mapFrom($payload);

                if (!is_array($map1) || empty($map1)) {
                    $itemsToRemove[] = $payload;
                    continue;
                }

                $runAt = Cast::toInt($map1['runAt']);

                if ($runAt > $now) {
                    continue;
                }

                $itemsToRemove[] = $payload;

                if ($now - $runAt <= 5) {
                    $payloads[] = $payload;
                }
            }

            if (!empty($itemsToRemove)) {
                try {
                    RedisCmd::zRem($cacheKey, ...$itemsToRemove);
                } catch (Throwable $ex) {
                }
            }

            foreach ($payloads as $payload) {
                call_user_func($taskExecutor, $server, $payload);
            }
        });
    }
}
