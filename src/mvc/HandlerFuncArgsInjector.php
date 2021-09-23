<?php

namespace mgboot\mvc;

use Lcobucci\JWT\Token;
use mgboot\http\server\Request;
use mgboot\http\server\UploadedFile;
use mgboot\common\util\ArrayUtils;
use mgboot\common\util\JsonUtils;
use mgboot\common\util\StringUtils;
use RuntimeException;
use stdClass;
use Throwable;

final class HandlerFuncArgsInjector
{
    private static $fmt1 = 'fail to inject arg for handler function %s, name: %s, type: %s';

    private function __construct()
    {
    }

    public static function inject(Request $req): array
    {
        $routeRule = $req->getRouteRule();
        $handler = $routeRule->getHandler();
        $args = [];

        foreach ($routeRule->getHandlerFuncArgs() as $info) {
            if ($info->isRequest()) {
                $args[] = $req;
                continue;
            }

            if ($info->isJwt()) {
                $jwt = $req->getJwt();

                if (!($jwt instanceof Token) && !$info->isNullable()) {
                    self::thowException($handler, $info);
                }

                $args[] = $jwt;
                continue;
            }

            if ($info->isClientIp()) {
                $args[] = $req->getClientIp();
                continue;
            }

            if ($info->getHttpHeaderName() !== '') {
                $args[] = $req->getHeader($info->getHttpHeaderName());
                continue;
            }

            if ($info->getJwtClaimName() !== '') {
                self::injectJwtClaim($req, $args, $info);
                continue;
            }

            if ($info->getPathVariableName() !== '') {
                self::injectPathVariable($req, $args, $info);
                continue;
            }

            if ($info->getRequestParamName() !== '') {
                self::injectRequestParam($req, $args, $info);
                continue;
            }

            if ($info->isParamMap()) {
                self::injectParamMap($req, $args, $info);
                continue;
            }

            if ($info->isUploadedFile()) {
                self::injectUploadedFile($req, $args, $info);
                continue;
            }

            if ($info->isNeedRequestBody()) {
                self::injectRequestBody($req, $args, $info);
                continue;
            }

            self::thowException($handler, $info);
        }

        return $args;
    }

    private static function injectJwtClaim(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        $claimName = $info->getJwtClaimName();

        switch ($info->getType()) {
            case 'int':
                $args[] = $req->jwtIntCliam($claimName);
                break;
            case 'float':
                $args[] = $req->jwtFloatClaim($claimName);
                break;
            case 'bool':
                $args[] = $req->jwtBooleanClaim($claimName);
                break;
            case 'string':
                $args[] = $req->jwtStringClaim($claimName);
                break;
            default:
                if ($info->isNullable()) {
                    $args[] = null;
                } else {
                    $fmt = '@@fmt:' . self::$fmt1 . ', reason: unsupported jwt claim type [%s]';
                    $handler = $req->getRouteRule()->getHandler();
                    self::thowException($handler, $info, $fmt, $info->getType());
                }

                break;
        }
    }

    private static function injectPathVariable(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        $name = $info->getPathVariableName();

        switch ($info->getType()) {
            case 'int':
                $args[] = $req->pathVariableAsInt($name);
                break;
            case 'float':
                $args[] = $req->pathVariableAsFloat($name);
                break;
            case 'bool':
                $args[] = $req->pathVariableAsBoolean($name);
                break;
            case 'string':
                $args[] = $req->pathVariableAsString($name);
                break;
            default:
                if ($info->isNullable()) {
                    $args[] = null;
                } else {
                    $fmt = '@@fmt:' . self::$fmt1 . ', reason: unsupported path variable type [%s]';
                    $handler = $req->getRouteRule()->getHandler();
                    self::thowException($handler, $info, $fmt, $info->getType());
                }

                break;
        }
    }

    private static function injectRequestParam(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        $name = $info->getRequestParamName();

        switch ($info->getType()) {
            case 'int':
                $args[] = $req->requestParamAsInt($name);
                break;
            case 'float':
                $args[] = $req->requestParamAsFloat($name);
                break;
            case 'bool':
                $args[] = $req->requestParamAsBoolean($name);
                break;
            case 'string':
                if ($info->isDecimal()) {
                    $args[] = bcadd($req->requestParamAsString($name), 0, 2);
                } else {
                    $args[] = trim($req->requestParamAsString($name, $info->getSecurityMode()));
                }

                break;
            default:
                if ($info->isNullable()) {
                    $args[] = null;
                } else {
                    $fmt = '@@fmt:' . self::$fmt1 . ', reason: unsupported request param type [%s]';
                    $handler = $req->getRouteRule()->getHandler();
                    self::thowException($handler, $info, $fmt, $info->getType());
                }

                break;
        }
    }

    private static function injectParamMap(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        $handler = $req->getRouteRule()->getHandler();

        if ($info->getType() !== 'array') {
            if ($info->isNullable()) {
                $args[] = null;
                return;
            }

            self::thowException($handler, $info);
        }

        $isGet = strtoupper($req->getMethod()) === 'GET';
        $contentType = $req->getHeader('Content-Type');
        $isJsonPayload = stripos($contentType, 'application/json') !== false;

        $isXmlPayload = stripos($contentType, 'application/xml') !== false ||
            stripos($contentType, 'text/xml') !== false;

        if ($isGet) {
            $map1 = $req->getQueryParams();
        } else if ($isJsonPayload) {
            $map1 = JsonUtils::mapFrom($req->getRawBody());
        } else if ($isXmlPayload) {
            $map1 = StringUtils::xml2assocArray($req->getRawBody());
        } else {
            $map1 = array_merge($req->getQueryParams(), $req->getFormData());
        }

        if (!is_array($map1)) {
            if ($info->isNullable()) {
                $args[] = null;
                return;
            }

            self::thowException($handler, $info);
        }

        foreach ($map1 as $key => $val) {
            if (!is_string($val) || is_numeric($val)) {
                continue;
            }

            $map1[$key] = trim($val);
        }

        $rules = $info->getParamMapRules();
        $args[] = empty($rules) ? $map1 : ArrayUtils::requestParams($map1, $rules);
    }

    private static function injectUploadedFile(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        $formFieldName = $info->getFormFieldName();

        try {
            $uploadFile = $req->getUploadedFiles()[$formFieldName];
        } catch (Throwable $ex) {
            $uploadFile = null;
        }

        if (!($uploadFile instanceof UploadedFile)) {
            if ($info->isNullable()) {
                $args[] = null;
                return;
            }

            $handler = $req->getRouteRule()->getHandler();
            self::thowException($handler, $info);
        }

        $args[] = $uploadFile;
    }

    private static function injectRequestBody(Request $req, array &$args, HandlerFuncArgInfo $info): void
    {
        if ($info->getType() !== 'string') {
            if ($info->isNullable()) {
                $args[] = null;
                return;
            }

            $handler = $req->getRouteRule()->getHandler();
            self::thowException($handler, $info);
        }

        $payload = $req->getRawBody();
        $map1 = JsonUtils::mapFrom($payload);

        if (is_array($map1) && ArrayUtils::isAssocArray($map1)) {
            foreach ($map1 as $key => $val) {
                if (!is_string($val) || is_numeric($val)) {
                    continue;
                }

                $map1[$key] = trim($val);
            }

            $payload = JsonUtils::toJson(empty($map1) ? new stdClass() : $map1);
        }

        $args[] = $payload;
    }

    /**
     * @param string $handler
     * @param HandlerFuncArgInfo $info
     * @param mixed ...$args
     */
    private static function thowException(string $handler, HandlerFuncArgInfo $info, ... $args): void
    {
        $fmt = self::$fmt1;
        $params = [$handler, $info->getName(), $info->getType()];

        if (!empty($args)) {
            if (is_string($args[0]) && StringUtils::startsWith($args[0], '@@fmt:')) {
                $fmt = str_replace('@@fmt:', '', array_shift($args));

                if (!empty($args)) {
                    array_push($params, ...$args);
                }
            } else {
                array_push($params, ...$args);
            }
        }

        $errorTips = sprintf($fmt, ...$params);
        throw new RuntimeException($errorTips);
    }
}
