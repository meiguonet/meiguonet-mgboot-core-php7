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
use mgboot\security\CorsSettings;
use mgboot\security\SecurityContext;
use Throwable;

final class MgBoot
{
    /**
     * @var bool
     */
    private static $_gzipOutputEnabled = true;

    /**
     * @var ExceptionHandler[]
     */
    private static $exceptionHandlers = [];

    /**
     * @var Middleware[]
     */
    private static $middlewares = [];

    /**
     * @var array
     */
    private static $controllerMap = [];

    private function __construct()
    {
    }

    public static function handleRequest(Request $request, Response $response, array $routeRules): void
    {
        $response->withExceptionHandlers(self::$exceptionHandlers);
        $corsSettings = SecurityContext::getCorsSettings();

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

        RequestHandler::create($request, $response)->handleRequest(self::$middlewares);
    }

    public static function gzipOutputEnabled(?bool $flag = null): bool
    {
        if (is_bool($flag)) {
            self::$_gzipOutputEnabled = $flag === true;
            return false;
        }

        return self::$_gzipOutputEnabled;
    }

    public static function withExceptionHandler(ExceptionHandler $handler): void
    {
        self::checkNecessaryExceptionHandlers();
        $idx = -1;

        foreach (self::$exceptionHandlers as $i => $item) {
            if ($item->getExceptionClassName() === $handler->getExceptionClassName()) {
                $idx = $i;
                break;
            }
        }

        if ($idx < 0) {
            self::$exceptionHandlers[] = $handler;
        } else {
            self::$exceptionHandlers[$idx] = $handler;
        }
    }

    public static function withMiddleware(Middleware $middleware): void
    {
        if (self::isMiddlewaresExists(get_class($middleware))) {
            return;
        }

        self::$middlewares[] = $middleware;
    }

    public static function buildControllerMap(array $routeRules): void
    {
        $server = Swoole::getServer();

        if (!is_object($server)) {
            return;
        }

        $key = 'worker' . Swoole::getWorkerId();
        $map1 = [];

        /* @var RouteRule $rule */
        foreach ($routeRules as $rule) {
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

        self::$controllerMap[$key] = $map1;
    }

    public static function getControllerBean(string $clazz)
    {
        $server = Swoole::getServer();

        if (!is_object($server)) {
            return null;
        }

        $key = 'worker' . Swoole::getWorkerId();

        if (!is_array(self::$controllerMap[$key])) {
            return null;
        }

        $bean = self::$controllerMap[$key][$clazz];
        return is_object($bean) ? $bean : null;
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

    private static function checkNecessaryExceptionHandlers(): void
    {
        $classes = [
            AccessTokenExpiredException::class,
            AccessTokenInvalidException::class,
            ValidateException::class,
            RequireAccessTokenException::class
        ];

        foreach ($classes as $clazz) {
            $found = false;

            foreach (self::$exceptionHandlers as $handler) {
                if (strpos($handler->getExceptionClassName(), $clazz) !== false) {
                    $found = true;
                    break;
                }
            }

            if (!$found) {
                self::$exceptionHandlers[] = ExceptionHandlerImpl::create($clazz);
            }
        }
    }

    private static function isMiddlewaresExists(string $clazz): bool
    {
        $clazz = StringUtils::ensureLeft($clazz, "\\");

        foreach (self::$middlewares as $mid) {
            if (StringUtils::ensureLeft(get_class($mid), "\\") === $clazz) {
                return true;
            }
        }

        return false;
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
}
