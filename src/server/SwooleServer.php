<?php

namespace mgboot\server;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\swoole\SwooleTable;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\FileUtils;
use mgboot\common\util\StringUtils;

final class SwooleServer
{
    const SETTINGS_KEYS = [
        'intKeys' => [
            'worker_num' => null,
            'max_request' => null,
            'task_worker_num' => null,
            'task_max_request' => null,
            'dispatch_mode' => 1,
            'log_level' => null,
            'log_rotation' => null,
            'package_max_length' => 1024 * 1024 * 64,
            'max_wait_time' => null,
            'hook_flags' => null,
            'http_compression_level' => null,
            'compression_min_length' => null
        ],
        'stringKeys' => [
            'log_file' => null,
            'log_date_format' => null,
            'ssl_cert_file' => null,
            'ssl_key_file' => null,
            'ssl_ciphers' => null,
            'ssl_client_cert_file' => null,
            'pid_file' => null,
            'upload_tmp_dir' => null
        ],
        'boolKeys' => [
            'task_use_object' => null,
            'daemonize' => null,
            'ssl_verify_peer' => null,
            'ssl_allow_self_signed' => null,
            'reload_async' => null,
            'enable_coroutine' => true,
            'http_parse_post' => null,
            'http_parse_cookie' => null,
            'http_parse_files' => null
        ]
    ];

    /**
     * @var \Swoole\Server|null $server
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private $server = null;

    private function __construct(array $settings)
    {
        if (empty($settings) || !ArrayUtils::isAssocArray($settings)) {
            return;
        }

        $settings = $this->handleSettings($settings);
        $host = Cast::toString($settings['host']);

        if (empty($host)) {
            return;
        }

        $port = Cast::toInt($settings['port']);

        if ($port < 1) {
            return;
        }

        $serverSettings = [];

        foreach ($settings as $key => $value) {
            foreach (self::SETTINGS_KEYS['intKeys'] as $sk => $defaultValue) {
                $compareKey = strtr($sk, ['-' => '', '_' => '']);

                if (strtolower($key) !== strtolower($compareKey)) {
                    continue;
                }

                $n1 = Cast::toInt($value);

                if ($n1 >= 0) {
                    $serverSettings[$sk] = $n1;
                } else if (is_int($defaultValue)) {
                    $serverSettings[$sk] = $defaultValue;
                }
            }

            foreach (self::SETTINGS_KEYS['stringKeys'] as $sk => $defaultValue) {
                $compareKey = strtr($sk, ['-' => '', '_' => '']);

                if (strtolower($key) !== strtolower($compareKey)) {
                    continue;
                }

                if (is_string($value) && $value !== '') {
                    $serverSettings[$sk] = $value;
                } else if (is_string($defaultValue)) {
                    $serverSettings[$sk] = $defaultValue;
                }
            }

            foreach (self::SETTINGS_KEYS['boolKeys'] as $sk => $defaultValue) {
                $compareKey = strtr($sk, ['-' => '', '_' => '']);

                if (strtolower($key) !== strtolower($compareKey)) {
                    continue;
                }

                if (is_bool($value)) {
                    $serverSettings[$sk] = $value;
                } else if (is_bool($defaultValue)) {
                    $serverSettings[$sk] = $defaultValue;
                }
            }
        }

        if (empty($serverSettings)) {
            return;
        }

        if ($settings['enableWebsocket']) {
            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            $server = new \Swoole\WebSocket\Server($host, $port);
        } else {
            /** @noinspection PhpFullyQualifiedNameUsageInspection */
            $server = new \Swoole\Http\Server($host, $port);
        }

        $server->set($serverSettings);

        if (is_string($settings['pidFile']) && $settings['pidFile'] !== '') {
            /** @noinspection PhpUndefinedFieldInspection */
            $server->masterPidFile = $settings['pidFile'];
        }

        if (is_int($settings['taskWorkerNum']) && $settings['taskWorkerNum'] > 0) {
            /** @noinspection PhpUndefinedFieldInspection */
            $server->withTaskWorker = true;
        }

        if ($settings['enableTaskServer']) {
            /** @noinspection PhpUndefinedFieldInspection */
            $server->enableTaskServer = true;
        }

        Swoole::withTable($server, SwooleTable::cacheTableName());
        Swoole::withTable($server, SwooleTable::poolTableName());
        Swoole::withTable($server, SwooleTable::wsTableName());
        Swoole::withTable($server, SwooleTable::cronTableName());
        Swoole::setServer($server);
        $this->server = $server;
    }

    public static function create(array $settings): self
    {
        return new self($settings);
    }

    public function withEventListener(string $eventName, callable $callback): self
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        if (!($this->server instanceof \Swoole\Server)) {
            return $this;
        }

        $this->server->on($eventName, $callback);
        return $this;
    }

    public function start(): void
    {
        /** @noinspection PhpFullyQualifiedNameUsageInspection */
        if (!($this->server instanceof \Swoole\Server)) {
            return;
        }

        $this->server->start();
    }

    public function getPidFile(): string
    {
        $server = $this->server;

        if (!is_object($server) || !property_exists($server, 'masterPidFile')) {
            return '';
        }

        return $server->masterPidFile;
    }

    private function handleSettings(array $settings): array
    {
        foreach ($settings as $key => $value) {
            $newKey = strtr($key, ['-' => ' ', '_' => ' ']);
            $newKey = str_replace(' ', '', ucwords($newKey));
            $newKey = lcfirst($newKey);

            if (is_string($value)) {
                if ($value === '') {
                    unset($settings[$key]);
                    continue;
                }

                if (StringUtils::startsWith($value, 'classpath:')) {
                    $value = FileUtils::getRealpath($value);
                    $settings[$key] = $value;
                } else if (StringUtils::startsWith($value, '@DataSize:')) {
                    $value = Cast::toDataSize($value);
                    $settings[$key] = $value;
                } else if (StringUtils::startsWith($value, '@Duration:')) {
                    $value = Cast::toDuration($value);
                    $settings[$key] = $value;
                }
            }

            if ($newKey !== $key) {
                $settings[$newKey] = $value;
                unset($settings[$key]);
            }
        }

        if (is_int($settings['taskWorkerNum']) && $settings['taskWorkerNum'] > 0) {
            $settings['taskUseObject'] = true;
        }

        if (is_string($settings['logFile'])) {
            $settings['logLevel'] = SWOOLE_LOG_WARNING;
            $settings['logRotation'] = SWOOLE_LOG_ROTATION_DAILY;
            $settings['logDateFormat'] = '%Y-%m-%d %H:%M:%S';
        }

        if ($settings['httpCompression'] === false) {
            unset($settings['httpCompressionLevel']);
        } else {
            if (!is_int($settings['httpCompressionLevel']) || $settings['httpCompressionLevel'] < 1 || $settings['httpCompressionLevel'] > 9) {
                $settings['httpCompressionLevel'] = 5;
            }
        }

        $settings['hookFlags'] = SWOOLE_HOOK_ALL | SWOOLE_HOOK_CURL;
        return $settings;
    }
}
