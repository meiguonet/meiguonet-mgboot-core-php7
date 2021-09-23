<?php

namespace mgboot;

use FastRoute\Dispatcher;
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
use mgboot\mvc\MvcContext;
use mgboot\mvc\RouteRule;
use mgboot\security\CorsSettings;
use mgboot\security\SecurityContext;
use Throwable;

final class MgBoot
{
    /**
     * @var string
     */
    private static $controllerDir = '';

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

    private function __construct()
    {
    }

    public static function handleRequest(Request $request, Response $response): void
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

        $dispatcher = MvcContext::getRouteDispatcher();

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

                    if (!self::setRouteRuleToRequest($request, $httpMethod, $handlerFunc)) {
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

    public static function scanControllersIn(string $dir): void
    {
        if ($dir === '' || !is_dir($dir)) {
            return;
        }

        self::$controllerDir = $dir;
    }

    public static function getControllerDir(): string
    {
        return self::$controllerDir;
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

    private static function setRouteRuleToRequest(Request $request, string $httpMethod, string $handlerFunc): bool
    {
        $routeRules = MvcContext::getRouteRules();
        $matched = null;

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
