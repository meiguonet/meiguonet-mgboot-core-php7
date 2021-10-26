<?php

namespace mgboot;

use mgboot\common\swoole\Swoole;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\StringUtils;
use mgboot\exception\AccessTokenExpiredException;
use mgboot\exception\AccessTokenInvalidException;
use mgboot\exception\ExceptionHandler;
use mgboot\exception\ExceptionHandlerImpl;
use mgboot\exception\HttpError;
use mgboot\exception\RequireAccessTokenException;
use mgboot\exception\ValidateException;
use mgboot\http\middleware\Middleware;
use mgboot\http\server\Request;
use mgboot\http\server\RequestHandler;
use mgboot\http\server\Response;
use mgboot\http\server\response\JsonResponse;
use mgboot\mvc\RouteRule;
use mgboot\mvc\RoutingContext;
use mgboot\security\CorsSettings;
use Throwable;

final class MgBoot
{
    /**
     * @var array
     */
    private static $map1 = [];

    private function __construct()
    {
    }

    public static function gzipOutputEnabled(?bool $flag = null, ?int $workerId = null): bool
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "gzipOutputEnabled_worker$workerId";
        } else {
            $key = 'gzipOutputEnabled_noworker';
        }

        if (is_bool($flag)) {
            self::$map1[$key] = $flag;
            return false;
        }

        return self::$map1[$key] === true;
    }

    public static function withExceptionHandler(ExceptionHandler $handler, ?int $workerId = null): void
    {
        self::checkNecessaryExceptionHandlers($workerId);

        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "exceptionHandlers_worker$workerId";
        } else {
            $key = 'exceptionHandlers_noworker';
        }

        if (!is_array(self::$map1[$key])) {
            self::$map1[$key] = [$handler];
            return;
        }

        $idx = -1;

        /* @var ExceptionHandler $item */
        foreach (self::$map1[$key] as $i => $item) {
            if ($item->getExceptionClassName() === $handler->getExceptionClassName()) {
                $idx = $i;
                break;
            }
        }

        if ($idx < 0) {
            self::$map1[$key][] = $handler;
        } else {
            self::$map1[$key][$idx] = $handler;
        }
    }

    public static function withMiddleware(Middleware $middleware, ?int $workerId = null): void
    {
        if (self::isMiddlewareExists(get_class($middleware), $workerId)) {
            return;
        }

        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "middlewares_worker$workerId";
        } else {
            $key = 'middlewares_noworker';
        }

        if (!is_array(self::$map1[$key])) {
            self::$map1[$key] = [$middleware];
        } else {
            self::$map1[$key][] = $middleware;
        }
    }

    public static function handleRequest(Request $request, Response $response, array $routeRules): void
    {
        $response->withExceptionHandlers(self::getExceptionHandlers());
        $corsSettings = CorsSettings::loadCurrent();

        if ($corsSettings instanceof CorsSettings) {
            $response->withCorsSettings($corsSettings);
        }

        if (strtolower($request->getMethod()) === 'options') {
            $response->withPayload(JsonResponse::withPayload(['status' => 200]))->send();
            return;
        }

        $httpMethod = strtoupper($request->getMethod());
        $uri = $request->getRequestUrl();

        if (strpos($uri, '?') !== false) {
            $uri = substr($uri, 0, strpos($uri, '?'));
        }

        $uri = StringUtils::ensureLeft(rawurldecode($uri), '/');
        list($errorCode, $handlerFunc, $pathVariables) = self::dispatch($httpMethod, $uri, $routeRules);

        if (is_int($errorCode) && $errorCode >= 400) {
            $response->withPayload(HttpError::create($errorCode))->send();
            return;
        }

        if (!is_string($handlerFunc) || $handlerFunc === '') {
            $response->withPayload(HttpError::create(400))->send();
            return;
        }

        if (!self::setRouteRuleToRequest($request, $httpMethod, $handlerFunc, $routeRules)) {
            $response->withPayload(HttpError::create(400))->send();
            return;
        }

        if (is_array($pathVariables) && !empty($pathVariables)) {
            $request->withPathVariables($pathVariables);
        }

        RequestHandler::create($request, $response)->handleRequest(self::getMiddlewares());
    }

    public static function withControllers(?int $workerId = null): void
    {
        if (!Swoole::inCoroutineMode(true)) {
            return;
        }

        if (!is_int($workerId) || $workerId < 0) {
            $workerId = Swoole::getWorkerId();
        }

        $key = "controllers_worker$workerId";

        if (isset(self::$map1[$key])) {
            return;
        }

        $map1 = [];

        foreach (RoutingContext::getRouteRules() as $rule) {
            list($clazz, $methodName) = explode('@', $rule->getHandler());
            unset($methodName);

            if (isset($map1[$clazz])) {
                continue;
            }

            try {
                $bean = new $clazz();
            } catch (Throwable $ex) {
                $bean = null;
            }

            if (!is_object($bean)) {
                continue;
            }

            $map1[$clazz] = $bean;
        }

        self::$map1[$key] = $map1;
    }

    public static function getControllerBean(string $clazz, ?int $workerId = null)
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId) || $workerId < 0) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "controllers_worker$workerId";
            return self::$map1[$key][$clazz];
        }

        try {
            $bean = new $clazz();
        } catch (Throwable $ex) {
            $bean = null;
        }

        return $bean;
    }

    private static function checkNecessaryExceptionHandlers(?int $workerId = null): void
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "exceptionHandlers_worker$workerId";
        } else {
            $key = 'exceptionHandlers_noworker';
        }

        $handlers = self::$map1[$key] ?? [];

        $classes = [
            AccessTokenExpiredException::class,
            AccessTokenInvalidException::class,
            ValidateException::class,
            RequireAccessTokenException::class
        ];

        foreach ($classes as $clazz) {
            $found = false;

            foreach ($handlers as $handler) {
                if (strpos($handler->getExceptionClassName(), $clazz) !== false) {
                    $found = true;
                    break;
                }
            }

            if (!$found) {
                $handlers[] = ExceptionHandlerImpl::create($clazz);
            }
        }

        self::$map1[$key] = $handlers;
    }

    private static function isMiddlewareExists(string $clazz, ?int $workerId = null): bool
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "middlewares_worker$workerId";
        } else {
            $key = 'middlewares_noworker';
        }

        if (isset(self::$map1[$key])) {
            $middlewares = self::$map1[$key];
        } else {
            self::$map1[$key] = [];
            return false;
        }

        $clazz = StringUtils::ensureLeft($clazz, "\\");

        foreach ($middlewares as $mid) {
            if (StringUtils::ensureLeft(get_class($mid), "\\") === $clazz) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param int|null $workerId
     * @return ExceptionHandler[]
     */
    private static function getExceptionHandlers(int $workerId = null): array
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "exceptionHandlers_worker$workerId";
        } else {
            $key = 'exceptionHandlers_noworker';
        }

        $handlers = self::$map1[$key];
        return is_array($handlers) ? $handlers : [];
    }

    private static function dispatch(string $httpMethod, string $uri, array $routeRules): array
    {
        $entries = [];

        /* @var RouteRule $rr */
        foreach ($routeRules as $rr) {
            $requestMapping = $rr->getRequestMapping();

            if ($requestMapping === $uri) {
                $entries[] = [
                    'httpMethod' => strtoupper($rr->getHttpMethod()),
                    'handler' => $rr->getHandler()
                ];

                continue;
            }

            if ($rr->getRegex() === '') {
                continue;
            }

            $pathVariableNames = $rr->getPathVariableNames();
            $matches = [];
            preg_match($rr->getRegex(), $uri, $matches);

            if (!ArrayUtils::isStringArray($matches) || count($matches) <= count($pathVariableNames)) {
                continue;
            }

            $pathVariables = [];

            foreach ($matches as $i => $m) {
                if ($i < 1) {
                    continue;
                }

                $pathVariables[$pathVariableNames[$i - 1]] = $m;
            }

            $entries[] = [
                'httpMethod' => strtoupper($rr->getHttpMethod()),
                'handler' => $rr->getHandler(),
                'pathVariables' => $pathVariables
            ];
        }

        if (empty($entries)) {
            return [404, null, null];
        }

        foreach ($entries as $entry) {
            if ($entry['httpMethod'] === $httpMethod || in_array($entry['httpMethod'], ['*', 'ALL'])) {
                return [null, $entry['handler'], is_array($entry['pathVariables']) ? $entry['pathVariables'] : []];
            }
        }

        return [405, null, null];
    }

    private static function setRouteRuleToRequest(
        Request $request,
        string $httpMethod,
        string $handlerFunc,
        array $routeRules
    ): bool
    {
        $matched = null;

        /* @var RouteRule $rule */
        foreach ($routeRules as $rule) {
            if ($rule->getHttpMethod() === $httpMethod && $rule->getHandler() === $handlerFunc) {
                $matched = $rule;
                break;
            }
        }

        if (!($matched instanceof RouteRule)) {
            foreach ($routeRules as $rule) {
                if ($rule->getHandler() === $handlerFunc && $rule->getHttpMethod() === '') {
                    $matched = $rule;
                    break;
                }
            }
        }

        if ($matched instanceof RouteRule) {
            $request->withRouteRule($matched);
            return true;
        }

        return false;
    }

    /**
     * @param int|null $workerId
     * @return Middleware[]
     */
    private static function getMiddlewares(int $workerId = null): array
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId)) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "middlewares_worker$workerId";
        } else {
            $key = 'middlewares_noworker';
        }

        $handlers = self::$map1[$key];
        return is_array($handlers) ? $handlers : [];
    }
}
