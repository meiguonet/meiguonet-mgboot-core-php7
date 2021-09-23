<?php

namespace mgboot\caching;

use mgboot\common\Cast;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\FileUtils;
use mgboot\common\util\SerializeUtils;
use Throwable;

class FileCache implements CacheInterface
{
    use CacheInterfaceTrait;

    /**
     * @var string
     */
    private $cacheDir = '';

    public function __construct(?string $cacheDir = null)
    {
        if (is_string($cacheDir) && $cacheDir !== '') {
            $this->cacheDir = $cacheDir;
        }
    }

    public function get(string $key, $default = null)
    {
        $cacheKey = $this->buildCacheKey($key);

        if (Swoole::inCoroutineMode(true)) {
            try {
                $cacheDir = $this->buildCacheDirAsync($cacheKey);
            } catch (Throwable $ex) {
                $cacheDir = '';
            }
        } else {
            $cacheDir = $this->buildCacheDir($cacheKey);
        }

        if ($cacheDir === '') {
            return $default;
        }

        $cacheFile = $this->getCacheFile($cacheDir, $cacheKey);

        if (!is_file($cacheFile)) {
            return $default;
        }

        if (Swoole::inCoroutineMode(true)) {
            try {
                $contents = $this->readFromFileAsync($cacheFile);
            } catch (Throwable $ex) {
                $contents = '';
            }
        } else {
            $contents = $this->readFromFile($cacheFile);
        }

        $entry = SerializeUtils::unserialize($contents);

        if (!ArrayUtils::isAssocArray($entry)) {
            return $default;
        }

        $expiry = Cast::toInt($entry['expiry']);

        if ($expiry > 0 && time() > $expiry) {
            unlink($cacheFile);
            return $default;
        }

        return $entry['value'];
    }

    public function set(string $key, $value, ?int $ttl = null): bool
    {
        $cacheKey = $this->buildCacheKey($key);

        if (Swoole::inCoroutineMode(true)) {
            try {
                $cacheDir = $this->buildCacheDirAsync($cacheKey);
            } catch (Throwable $ex) {
                $cacheDir = '';
            }
        } else {
            $cacheDir = $this->buildCacheDir($cacheKey);
        }

        if ($cacheDir === '') {
            return false;
        }

        $cacheFile = $this->getCacheFile($cacheDir, $cacheKey);
        $ttl = Cast::toInt($ttl);
        $entry = ['value' => $value];

        if ($ttl > 0) {
            $entry['expiry'] = time() + $ttl;
        }

        $contents = SerializeUtils::serialize($entry);

        if (!is_string($contents) || empty($contents)) {
            return false;
        }

        if (Swoole::inCoroutineMode(true)) {
            try {
                $this->writeToFileAsync($cacheFile, $contents);
                return true;
            } catch (Throwable $ex) {
                return false;
            }
        }

        $this->writeToFile($cacheFile, $contents);
        return true;
    }

    public function delete(string $key): bool
    {
        $cacheKey = $this->buildCacheKey($key);

        if (Swoole::inCoroutineMode(true)) {
            try {
                $cacheDir = $this->buildCacheDirAsync($cacheKey);
            } catch (Throwable $ex) {
                $cacheDir = '';
            }
        } else {
            $cacheDir = $this->buildCacheDir($cacheKey);
        }

        if ($cacheDir === '') {
            return false;
        }

        $cacheFile = $this->getCacheFile($cacheDir, $cacheKey);
        is_file($cacheFile) && unlink($cacheFile);
        return true;
    }

    public function clear(): bool
    {
        return true;
    }

    public function has(string $key): bool
    {
        $cacheKey = $this->buildCacheKey($key);

        if (Swoole::inCoroutineMode(true)) {
            try {
                $cacheDir = $this->buildCacheDirAsync($cacheKey);
            } catch (Throwable $ex) {
                $cacheDir = '';
            }
        } else {
            $cacheDir = $this->buildCacheDir($cacheKey);
        }

        if ($cacheDir === '') {
            return false;
        }

        $cacheFile = $this->getCacheFile($cacheDir, $cacheKey);
        return is_file($cacheFile);
    }

    private function buildCacheDir(string $cacheKey): string
    {
        $dir = FileUtils::getRealpath($this->cacheDir);

        if (is_dir($dir)) {
            if (!is_writable($dir)) {
                return '';
            }
        } else {
            mkdir($dir, 0755, true);

            if (!is_dir($dir)) {
                return '';
            }
        }

        $cacheKey = strtolower(md5($cacheKey));
        $d1 = substr($cacheKey, 0, 2);
        $d2 = substr($cacheKey, -2);
        $dir = "$dir/$d1/$d2";

        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }

        return is_dir($dir) ? $dir : '';
    }

    private function buildCacheDirAsync(string $cacheKey): string
    {
        $wg = Swoole::newWaitGroup();
        $wg->add();
        $baseDir = FileUtils::getRealpath($this->cacheDir);
        $cacheDir = '';

        Swoole::runInCoroutine(function () use ($wg, $baseDir, &$cacheDir, $cacheKey) {
            Swoole::defer(function () use ($wg) {
                $wg->done();
            });

            if (is_dir($baseDir)) {
                if (!is_writable($baseDir)) {
                    return;
                }
            } else {
                mkdir($baseDir, 0755, true);

                if (!is_dir($baseDir)) {
                    return;
                }
            }

            $cacheKey = strtolower(md5($cacheKey));
            $d1 = substr($cacheKey, 0, 2);
            $d2 = substr($cacheKey, -2);
            $cacheDir = "$baseDir/$d1/$d2";

            if (!is_dir($cacheDir)) {
                mkdir($cacheDir, 0755, true);
            }

            if (is_dir($cacheDir)) {
                $cacheDir = '';
            }
        });

        $wg->wait();
        return $cacheDir;
    }

    private function getCacheFile(string $cacheDir, string $cacheKey): string
    {
        $cacheKey = strtolower(md5($cacheKey));
        return "$cacheDir/$cacheKey.dat";
    }

    private function readFromFile(string $filepath): string
    {
        $contents = file_get_contents($filepath);
        return is_string($contents) ? $contents : '';
    }

    private function readFromFileAsync(string $filepath): string
    {
        $wg = Swoole::newWaitGroup();
        $wg->add();
        $contents = '';

        Swoole::runInCoroutine(function () use ($wg, $filepath, &$contents) {
            Swoole::defer(function () use ($wg) {
                $wg->done();
            });

            $contents = file_get_contents($filepath);

            if (!is_string($contents)) {
                $contents = '';
            }
        });

        $wg->wait();
        return $contents;
    }

    private function writeToFile(string $filepath, string $contents): void
    {
        $fp = fopen($filepath, 'w');
        flock($fp, LOCK_EX);
        fwrite($fp, $contents);
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    private function writeToFileAsync(string $filepath, string $contents): void
    {
        $wg = Swoole::newWaitGroup();
        $wg->add();

        Swoole::runInCoroutine(function () use ($wg, $filepath, $contents) {
            Swoole::defer(function () use ($wg) {
                $wg->done();
            });

            $fp = fopen($filepath, 'w');
            flock($fp, LOCK_EX);
            fwrite($fp, $contents);
            flock($fp, LOCK_UN);
            fclose($fp);
        });

        $wg->wait();
    }
}
