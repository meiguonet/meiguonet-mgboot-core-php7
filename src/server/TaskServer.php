<?php

namespace mgboot\server;

use Carbon\Carbon;
use Cron\CronExpression;
use DateTime;
use DateTimeZone;
use mgboot\annotation\Scheduled;
use mgboot\common\AppConf;
use mgboot\common\Cast;
use mgboot\common\constant\TimeUnit;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\ExceptionUtils;
use mgboot\common\util\FileUtils;
use mgboot\common\util\JsonUtils;
use mgboot\common\util\ReflectUtils;
use mgboot\common\util\StringUtils;
use mgboot\common\util\TokenizeUtils;
use mgboot\dal\redis\RedisCmd;
use mgboot\task\CronTask;
use mgboot\task\RetryPolicy;
use mgboot\task\Task;
use mgboot\task\TaskPublisher;
use Psr\Log\LoggerInterface;
use ReflectionClass;
use Symfony\Component\Process\PhpExecutableFinder;
use Symfony\Component\Process\PhpProcess;
use Throwable;
use Workerman\Timer;
use Workerman\Worker;

final class TaskServer
{
    /**
     * @var string
     */
    private static $cronTaskSourceDir = 'classpath:cron';

    /**
     * @var array
     */
    private static $cronTasks = [];

    /**
     * @var string
     */
    private static $phpBin = '';

    private static $cronTaskExecutor = 'classpath:run_cron_task.php';

    private static $taskExecutor = 'classpath:run_task.php';

    /**
     * @var LoggerInterface|null
     */
    private static $logger = null;

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

        $phpBin = self::$phpBin;

        if ($phpBin === '' || !is_file($phpBin) || !is_executable($phpBin)) {
            $phpBin = (new PhpExecutableFinder())->find();

            if (!is_string($phpBin) || $phpBin === '' || !is_file($phpBin) || !is_executable($phpBin)) {
                return false;
            }
        }

        $fpath = self::$cronTaskExecutor;

        if (empty($fpath)) {
            return false;
        }

        $fpath = FileUtils::getRealpath($fpath);

        if (!is_file($fpath)) {
            return false;
        }

        $cronTaskExecutorScripts = file_get_contents($fpath);

        if (!is_string($cronTaskExecutorScripts) || empty($cronTaskExecutorScripts)) {
            return false;
        }

        $fpath = self::$taskExecutor;

        if (empty($fpath)) {
            return false;
        }

        $fpath = FileUtils::getRealpath($fpath);

        if (!is_file($fpath)) {
            return false;
        }

        $taskExecutorScripts = file_get_contents($fpath);

        if (!is_string($taskExecutorScripts) || empty($taskExecutorScripts)) {
            return false;
        }

        self::buildCronTasks();
        $worker = new Worker();

        try {
            $worker->count = 1;
        } catch (Throwable $ex) {
        }

        $scripts = [$cronTaskExecutorScripts, $taskExecutorScripts];

        $worker->onWorkerStart = function () use ($phpBin, $scripts) {
            foreach (TaskServer::getCronTasks(true) as $item) {
                if (!is_int($item['interval'])) {
                    continue;
                }

                /* @var string $taskClass */
                $taskClass = $item['taskClass'];

                Timer::add(floatval($item['interval']), function () use ($phpBin, $scripts, $taskClass) {
                    TaskServer::handleCronTaskInWorkermenMode($phpBin, $scripts[0], $taskClass);
                });
            }

            Timer::add(1.0, function () use ($phpBin, $scripts) {
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
                    TaskServer::handleCronTaskInWorkermenMode($phpBin, $scripts[0], $item['taskClass']);
                }
            });

            Timer::add(2.0, function () use ($phpBin, $scripts) {
                $n1 = 20;
                $n2 = 1;
                $payloads = [];

                while (true) {
                    if ($n2 > $n1) {
                        break;
                    }

                    try {
                        $payload = RedisCmd::lPop(TaskPublisher::normalQueueCacheKey());
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
                    TaskServer::handleTaskInWorkermenMode($phpBin, $scripts[1], $payload);
                }
            });

            Timer::add(2.0, function () use ($phpBin, $scripts) {
                $now = Carbon::now(new DateTimeZone('Asia/Shanghai'))->timestamp;

                try {
                    $entries = RedisCmd::zRangeByScore(TaskPublisher::delayableQueueCacheKey(), $now - 3600, $now + 30);
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
                        RedisCmd::zRem(TaskPublisher::delayableQueueCacheKey(), ...$itemsToRemove);
                    } catch (Throwable $ex) {
                    }
                }

                foreach ($payloads as $payload) {
                    TaskServer::handleTaskInWorkermenMode($phpBin, $scripts[1], $payload);
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

            if (!($anno instanceof Scheduled)) {
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

    public static function withPhpBin(string $filepath): void
    {
        if (!is_file($filepath) || !is_executable($filepath)) {
            return;
        }

        self::$phpBin = $filepath;
    }

    public static function withCronTaskExecutor(string $filepath): void
    {
        self::$cronTaskExecutor = $filepath;
    }

    public static function withTaskExecutor(string $filepath): void
    {
        self::$taskExecutor = $filepath;
    }

    public static function withLogger(LoggerInterface $logger): void
    {
        self::$logger = $logger;
    }

    public static function handleCronTaskInSwooleMode($server, string $taskClass): void
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        if (!($server instanceof \Swoole\Server)) {
            return;
        }

        if (property_exists($server, 'withTaskWorker') && $server->withTaskWorker === true) {
            $server->task("@@cronTask:$taskClass");
            return;
        }

        go(function () use ($taskClass) {
            TaskServer::runCronTask($taskClass);
        });
    }

    public static function handleTaskInSwooleMode($server, string $payload): void
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        if (!($server instanceof \Swoole\Server)) {
            return;
        }

        if (property_exists($server, 'withTaskWorker') && $server->withTaskWorker === true) {
            $server->task("@@task:$payload");
            return;
        }

        go(function () use ($payload) {
            TaskServer::runTask($payload);
        });
    }

    public static function handleCronTaskInWorkermenMode(string $phpBin, string $scripts, string $taskClass): void
    {
        $process = self::buildProcess($phpBin, $scripts, $taskClass);

        if ($process === null) {
            return;
        }

        $process->start();
    }

    public static function handleTaskInWorkermenMode(string $phpBin, string $scripts, string $payload): void
    {
        $process = self::buildProcess($phpBin, $scripts, '', $payload);

        if ($process === null) {
            return;
        }

        $process->start();
    }

    public static function runCronTask(string $taskClass): void
    {
        $taskClass = str_replace('/', "\\", $taskClass);
        $taskClass = StringUtils::ensureLeft($taskClass, "\\");

        try {
            $task = new $taskClass();
        } catch (Throwable $ex) {
            $task = null;
        }

        if (!($task instanceof CronTask)) {
            return;
        }

        self::writeLog('info', "run cron task, task class: $taskClass");

        try {
            $task->run();
        } catch (Throwable $ex) {
        }
    }

    public static function runTask(string $payload): void
    {
        $map1 = JsonUtils::mapFrom($payload);

        if (!is_array($map1) || empty($map1)) {
            return;
        }

        $taskClass = Cast::toString($map1['taskClass']);

        if (empty($taskClass)) {
            return;
        }

        $taskClass = str_replace('/', "\\", $taskClass);
        $taskClass = StringUtils::ensureLeft($taskClass, "\\");
        $taskParams = is_array($map1['taskParams']) && !empty($map1['taskParams']) ? $map1['taskParams'] : [];
        $isDelayable = isset($map1['runAt']);

        try {
            $task = new $taskClass($taskParams);
        } catch (Throwable $ex) {
            $task = null;
        }

        if (!($task instanceof Task)) {
            return;
        }

        $msg = sprintf(
            'run %s task, task class: %s%s',
            $isDelayable ? 'delayable' : 'normal',
            $taskClass,
            empty($taskParams) ? '' : ', task params: ' . JsonUtils::toJson($taskParams)
        );

        self::writeLog('info', $msg);

        try {
            $success = $task->process();
        } catch (Throwable $ex) {
            self::writeLog('error', $ex);
            $success = false;
        }

        if ($success) {
            return;
        }

        $retryAttempts = Cast::toInt($map1['retryAttempts']);
        $retryInterval = Cast::toInt($map1['retryInterval']);

        if ($retryAttempts < 1 || $retryInterval < 1) {
            return;
        }

        $failTimes = Cast::toInt($map1['failTimes']);

        if ($failTimes < 1) {
            $failTimes = 1;
        } else {
            $failTimes += 1;
        }

        if ($failTimes > $retryAttempts) {
            return;
        }

        TaskPublisher::publishDelayableWithDelayAmount(
            $taskClass,
            $taskParams,
            $retryInterval,
            TimeUnit::SECONDS,
            RetryPolicy::create($failTimes, $retryAttempts, $retryInterval)
        );
    }

    /** @noinspection PhpFullyQualifiedNameUsageInspection */
    private static function runInSwooleMode(\Swoole\Server $server): void
    {
        foreach (self::getCronTasks(true) as $item) {
            if (!is_int($item['interval'])) {
                continue;
            }

            /* @var string $taskClass */
            $taskClass = $item['taskClass'];

            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            \Swoole\Timer::tick($item['interval'] * 1000, function () use ($server, $taskClass) {
                TaskServer::handleCronTaskInSwooleMode($server, $taskClass);
            });
        }

        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        \Swoole\Timer::tick(1000, function () use ($server) {
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
                TaskServer::handleCronTaskInSwooleMode($server, $item['taskClass']);
            }
        });

        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        \Swoole\Timer::tick(2000, function () use ($server) {
            $n1 = 20;
            $n2 = 1;
            $payloads = [];

            while (true) {
                if ($n2 > $n1) {
                    break;
                }

                try {
                    $payload = RedisCmd::lPop(TaskPublisher::normalQueueCacheKey());
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
                TaskServer::handleTaskInSwooleMode($server, $payload);
            }
        });

        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        \Swoole\Timer::tick(2000, function () use ($server) {
            $now = Carbon::now(new DateTimeZone('Asia/Shanghai'))->timestamp;

            try {
                $entries = RedisCmd::zRangeByScore(TaskPublisher::delayableQueueCacheKey(), $now - 3600, $now + 30);
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
                    RedisCmd::zRem(TaskPublisher::delayableQueueCacheKey(), ...$itemsToRemove);
                } catch (Throwable $ex) {
                }
            }

            foreach ($payloads as $payload) {
                TaskServer::handleTaskInSwooleMode($server, $payload);
            }
        });
    }

    private static function buildProcess(string $phpBin, string $scripts, string $taskClass, ?string $payload = null): ?PhpProcess
    {
        $rootPath = self::getRootPath();

        if (empty($rootPath)) {
            return null;
        }

        $scripts = str_replace('{rootPath}', '', $scripts);
        $scripts = str_replace('{env}', '', AppConf::getEnv(), $scripts);
        $scripts = str_replace('{taskClass}', $taskClass, $scripts);

        if (is_string($payload)) {
            $scripts = str_replace('{payload}', $payload, $scripts);
        }

        return new PhpProcess($scripts, $rootPath, null, 600, [$phpBin]);
    }

    private static function getRootPath(): string
    {
        if (defined('_ROOT_')) {
            $dir = _ROOT_;

            if (is_dir($dir)) {
                $dir = str_replace("\\", '/', $dir);
                return $dir === '/' ? $dir : rtrim($dir, '/');
            }
        }

        $dir = __DIR__;

        if (!is_dir($dir)) {
            return '';
        }

        while (true) {
            $dir = str_replace("\\", '/', $dir);

            if ($dir !== '/') {
                $dir = trim($dir, '/');
            }

            if (StringUtils::endsWith($dir, '/vendor')) {
                break;
            }

            $dir = realpath("$dir/../");

            if (!is_string($dir) || $dir === '' || !is_dir($dir)) {
                return '';
            }
        }

        $dir = str_replace("\\", '/', $dir);

        if ($dir !== '/') {
            $dir = trim($dir, '/');
        }

        $dir = realpath("$dir/../");

        if (!is_string($dir) || $dir === '' || !is_dir($dir)) {
            return '';
        }

        $dir = str_replace("\\", '/', $dir);
        return $dir === '/' ? $dir : rtrim($dir, '/');
    }

    private static function writeLog(string $level, $msg): void
    {
        $logger = self::$logger;

        if (!($logger instanceof LoggerInterface)) {
            return;
        }

        if ($msg instanceof Throwable) {
            $msg = ExceptionUtils::getStackTrace($msg);
        }

        if (!is_string($msg) || $msg === '') {
            return;
        }

        switch ($level) {
            case 'debug':
                $logger->debug($msg);
                break;
            case 'info':
                $logger->info($msg);
                break;
            case 'error':
                $logger->error($msg);
                break;
        }
    }
}
