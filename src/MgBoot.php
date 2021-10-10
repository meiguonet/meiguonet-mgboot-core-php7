<?php

namespace mgboot;

use FastRoute\Dispatcher;
use FastRoute\RouteCollector;
use mgboot\common\AppConf;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\FileUtils;
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
use mgboot\logging\Log;
use mgboot\mvc\RouteRule;
use mgboot\security\CorsSettings;
use mgboot\security\SecurityContext;
use Throwable;
use function FastRoute\cachedDispatcher;
use function FastRoute\simpleDispatcher;

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

        $dispatcher = self::buildRouteDispatcher($routeRules);

        if (!($dispatcher instanceof Dispatcher)) {
            $response->withPayload(HttpError::create(400))->send();
            return;
        }

        $httpMethod = strtoupper($request->getMethod());
        $uri = $request->getRequestUrl();

        if (strpos($uri, '?') !== false) {
            $uri = substr($uri, 0, strpos($uri, '?'));
        }

        $uri = rawurldecode($uri);

        try {
            list($resultCode, $handlerFunc, $pathVariables) = $dispatcher->dispatch($httpMethod, $uri);

            switch ($resultCode) {
                case Dispatcher::NOT_FOUND:
                    if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                        $msg = "$httpMethod $uri, 404 not found";
                        Log::info($msg);
                    }

                    $response->withPayload(HttpError::create(404))->send();
                    break;
                case Dispatcher::METHOD_NOT_ALLOWED:
                    $response->withPayload(HttpError::create(405))->send();
                    break;
                case Dispatcher::FOUND:
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
                    break;
                default:
                    $response->withPayload(HttpError::create(400))->send();
                    break;
            }
        } catch (Throwable $ex) {
            $response->withPayload($ex)->send();
        }
    }

    private static function buildRouteDispatcher(array $routeRules): ?Dispatcher
    {
        if (empty($routeRules)) {
            return null;
        }

        $cacheEnabled = true;
        $cacheDir = FileUtils::getRealpath('classpath:cache');

        if ($cacheDir !== '') {
            $cacheDir = rtrim(str_replace("\\", '/', $cacheDir), '/');
        }

        if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('app.force-dispatcher-cache')) {
            $cacheEnabled = false;
        } else if ($cacheDir === '' || !is_dir($cacheDir) || !is_writable($cacheDir)) {
            $cacheEnabled = false;
        }

        if (!$cacheEnabled) {
            simpleDispatcher(function (RouteCollector $r) use ($routeRules) {
                /* @var RouteRule $rule */
                foreach ($routeRules as $rule) {
                    switch ($rule->getHttpMethod()) {
                        case 'GET':
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: GET {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->get($rule->getRequestMapping(), $rule->getHandler());
                            break;
                        case 'POST':
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: POST {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->post($rule->getRequestMapping(), $rule->getHandler());
                            break;
                        case 'PUT':
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: PUT {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->put($rule->getRequestMapping(), $rule->getHandler());
                            break;
                        case 'PATCH':
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: PATCH {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->patch($rule->getRequestMapping(), $rule->getHandler());
                            break;
                        case 'DELETE':
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: DELETE {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->delete($rule->getRequestMapping(), $rule->getHandler());
                            break;
                        default:
                            if (AppConf::getEnv() === 'dev' && !AppConf::getBoolean('logging.disable-mgboot-debug-log')) {
                                $msg = "dispatch rule: GET or POST {$rule->getRequestMapping()}, handler: {$rule->getHandler()}";
                                Log::debug($msg);
                            }

                            $r->get($rule->getRequestMapping(), $rule->getHandler());
                            $r->post($rule->getRequestMapping(), $rule->getHandler());
                            break;
                    }
                }
            });
        }

        $suffix = Swoole::buildGlobalVarKey();
        $cacheFile = "$cacheDir/fastroute-$suffix.dat";

        return cachedDispatcher(function (RouteCollector $r) use ($routeRules) {
            /* @var RouteRule $rule */
            foreach ($routeRules as $rule) {
                switch ($rule->getHttpMethod()) {
                    case 'GET':
                        $r->get($rule->getRequestMapping(), $rule->getHandler());
                        break;
                    case 'POST':
                        $r->post($rule->getRequestMapping(), $rule->getHandler());
                        break;
                    case 'PUT':
                        $r->put($rule->getRequestMapping(), $rule->getHandler());
                        break;
                    case 'PATCH':
                        $r->patch($rule->getRequestMapping(), $rule->getHandler());
                        break;
                    case 'DELETE':
                        $r->delete($rule->getRequestMapping(), $rule->getHandler());
                        break;
                    default:
                        $r->get($rule->getRequestMapping(), $rule->getHandler());
                        $r->post($rule->getRequestMapping(), $rule->getHandler());
                        break;
                }
            }
        }, compact('cacheFile'));
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
