<?php

namespace mgboot\mvc;

use Doctrine\Common\Annotations\AnnotationReader;
use Lcobucci\JWT\Token;
use mgboot\annotation\ClientIp;
use mgboot\annotation\DeleteMapping;
use mgboot\annotation\GetMapping;
use mgboot\annotation\HttpHeader;
use mgboot\annotation\JwtAuth;
use mgboot\annotation\JwtClaim;
use mgboot\annotation\MapBind;
use mgboot\annotation\ParamInject;
use mgboot\annotation\PatchMapping;
use mgboot\annotation\PathVariable;
use mgboot\annotation\PostMapping;
use mgboot\annotation\PutMapping;
use mgboot\annotation\RequestBody;
use mgboot\annotation\RequestMapping;
use mgboot\annotation\RequestParam;
use mgboot\annotation\UploadedFile;
use mgboot\annotation\Validate;
use mgboot\http\server\Request;
use mgboot\MgBoot;
use mgboot\common\util\FileUtils;
use mgboot\common\util\ReflectUtils;
use mgboot\common\util\StringUtils;
use mgboot\common\util\TokenizeUtils;
use ReflectionClass;
use ReflectionMethod;
use ReflectionNamedType;
use Throwable;

final class RouteRulesBuilder
{
    private function __construct()
    {
    }

    /**
     * @return RouteRule[]
     */
    public static function buildRouteRules(): array
    {
        $dir = MgBoot::getControllerDir();

        if ($dir === '' || !is_dir($dir)) {
            return [];
        }

        $files = [];
        FileUtils::scanFiles($dir, $files);
        $rules = [];

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

            $anno1 = ReflectUtils::getClassAnnotation($clazz, RequestMapping::class);

            try {
                $methods = $clazz->getMethods(ReflectionMethod::IS_PUBLIC);
            } catch (Throwable $ex) {
                $methods = [];
            }

            foreach ($methods as $method) {
                try {
                    $map1 = array_merge(
                        [
                            'handler' => "$className@{$method->getName()}",
                            'handlerFuncArgs' => self::buildHandlerFuncArgs($method)
                        ],
                        self::buildValidateRules($method),
                        self::buildJwtAuthSettings($method),
                        self::buildExtraAnnotations($method)
                    );
                } catch (Throwable $ex) {
                    continue;
                }

                $rule = self::buildRouteRule(GetMapping::class, $method, $anno1, $map1);

                if ($rule instanceof RouteRule) {
                    $rules[] = $rule;
                    continue;
                }

                $rule = self::buildRouteRule(PostMapping::class, $method, $anno1, $map1);

                if ($rule instanceof RouteRule) {
                    $rules[] = $rule;
                    continue;
                }

                $rule = self::buildRouteRule(PutMapping::class, $method, $anno1, $map1);

                if ($rule instanceof RouteRule) {
                    $rules[] = $rule;
                    continue;
                }

                $rule = self::buildRouteRule(PatchMapping::class, $method, $anno1, $map1);

                if ($rule instanceof RouteRule) {
                    $rules[] = $rule;
                    continue;
                }

                $rule = self::buildRouteRule(DeleteMapping::class, $method, $anno1, $map1);

                if ($rule instanceof RouteRule) {
                    $rules[] = $rule;
                    continue;
                }

                $items = self::buildRouteRulesForRequestMapping($method, $anno1, $map1);

                if (!empty($items)) {
                    array_push($rules, ...$items);
                }
            }
        }

        return $rules;
    }

    private static function buildRouteRule(
        string $clazz,
        ReflectionMethod $method,
        $annoRequestMapping,
        array $data
    ): ?RouteRule
    {
        switch (StringUtils::substringAfterLast($clazz, "\\")) {
            case 'GetMapping':
                $httpMethod = 'GET';
                break;
            case 'PostMapping':
                $httpMethod = 'POST';
                break;
            case 'PutMapping':
                $httpMethod = 'PUT';
                break;
            case 'PatchMapping':
                $httpMethod = 'PATCH';
                break;
            case 'DeleteMapping':
                $httpMethod = 'DELETE';
                break;
            default:
                $httpMethod = '';
                break;
        }

        if ($httpMethod === '') {
            return null;
        }

        try {
            $newAnno =  ReflectUtils::getMethodAnnotation($method, $clazz);

            if (!is_object($newAnno) || !method_exists($newAnno, 'getValue')) {
                return null;
            }

            $data = array_merge(
                $data,
                self::buildRequestMapping($annoRequestMapping, $newAnno->getValue()),
                compact('httpMethod')
            );

            return RouteRule::create($data);
        } catch (Throwable $ex) {
            return null;
        }
    }

    /**
     * @param ReflectionMethod $method
     * @param mixed $annoRequestMapping
     * @param array $data
     * @return RouteRule[]
     */
    private static function buildRouteRulesForRequestMapping(
        ReflectionMethod $method,
        $annoRequestMapping,
        array $data
    ): array
    {
        try {
            $newAnno =  ReflectUtils::getMethodAnnotation($method, RequestMapping::class);

            if (!is_object($newAnno) || !method_exists($newAnno, 'getValue')) {
                return [];
            }

            $map1 = self::buildRequestMapping($annoRequestMapping, $newAnno->getValue());

            return [
                RouteRule::create(array_merge($data, $map1, ['httpMethod' => 'GET'])),
                RouteRule::create(array_merge($data, $map1, ['httpMethod' => 'POST']))
            ];
        } catch (Throwable $ex) {
            return [];
        }
    }

    /**
     * @param mixed $annoRequestMapping
     * @param string $requestMapping
     * @return array
     */
    private static function buildRequestMapping($annoRequestMapping, string $requestMapping): array
    {
        $requestMapping = preg_replace('/[\x20\t]+/', '', $requestMapping);
        $requestMapping = trim($requestMapping, '/');

        if ($annoRequestMapping instanceof RequestMapping) {
            $s1 = preg_replace('/[\x20\t]+/', '', $annoRequestMapping->getValue());

            if (!empty($s1)) {
                $requestMapping = trim($s1, '/') . '/' . $requestMapping;
            }
        }

        $requestMapping = StringUtils::ensureLeft($requestMapping, '/');
        return compact('requestMapping');
    }

    /**
     * @param ReflectionMethod $method
     * @return HandlerFuncArgInfo[]
     */
    private static function buildHandlerFuncArgs(ReflectionMethod $method): array
    {
        $params = $method->getParameters();
        $anno1 = ReflectUtils::getMethodAnnotation($method, ParamInject::class);

        if ($anno1 instanceof ParamInject) {
            $injectRules = $anno1->getValue();
        } else {
            $injectRules = [];
        }

        $n1 = count($injectRules) - 1;

        foreach ($params as $i => $p) {
            $type = $p->getType();

            if (!($type instanceof ReflectionNamedType)) {
                $params[$i] = HandlerFuncArgInfo::create(['name' => $p->getName()]);
                continue;
            }

            $typeName = $type->isBuiltin() ? $type->getName() : StringUtils::ensureLeft($type->getName(), "\\");

            $map1 = [
                'name' => $p->getName(),
                'type' => $typeName
            ];

            if ($type->allowsNull()) {
                $map1['nullable'] = true;
            }

            if (strpos($typeName, Request::class) !== false) {
                $map1['request'] = true;
                $params[$i] = HandlerFuncArgInfo::create($map1);
                continue;
            }

            if (strpos($typeName, Token::class) !== false) {
                $map1['jwt'] = true;
                $params[$i] = HandlerFuncArgInfo::create($map1);
                continue;
            }

            if ($i <= $n1) {
                $anno = $injectRules[$i];

                if ($anno instanceof ClientIp) {
                    $map1['clientIp'] = true;
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof HttpHeader) {
                    $map1['httpHeaderName'] = $anno->getName();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof JwtClaim) {
                    $map1['jwtClaimName'] = empty($anno->getName()) ? $p->getName() : $anno->getName();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof RequestParam) {
                    $map1['requestParamName'] = empty($anno->getName()) ? $p->getName() : $anno->getName();
                    $map1['decimal'] = $anno->isDecimal();
                    $map1['securityMode'] = $anno->getSecurityMode();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof PathVariable) {
                    $map1['pathVariableName'] = empty($anno->getName()) ? $p->getName() : $anno->getName();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof MapBind) {
                    $map1['paramMap'] = true;
                    $map1['paramMapRules'] = $anno->getRules();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof UploadedFile) {
                    $map1['uploadedFile'] = true;
                    $map1['formFieldName'] = $anno->getValue();
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }

                if ($anno instanceof RequestBody) {
                    $map1['needRequestBody'] = true;
                    $params[$i] = HandlerFuncArgInfo::create($map1);
                    continue;
                }
            }

            $params[$i] = HandlerFuncArgInfo::create($map1);
        }

        return $params;
    }

    private static function buildJwtAuthSettings(ReflectionMethod $method): array
    {
        $anno =  ReflectUtils::getMethodAnnotation($method, JwtAuth::class);
        return $anno instanceof JwtAuth ? ['jwtSettingsKey' => $anno->getValue()] : [];
    }

    private static function buildValidateRules(ReflectionMethod $method): array
    {
        $anno = ReflectUtils::getMethodAnnotation($method, Validate::class);
        return $anno instanceof Validate ? ['validateRules' => $anno->getRules(), 'failfast' => $anno->isFailfast()] : [];
    }

    private static function buildExtraAnnotations(ReflectionMethod $method): array
    {
        try {
            $reader = new AnnotationReader();
            $annos = $reader->getMethodAnnotations($method);
        } catch (Throwable $ex) {
            $annos = [];
        }

        $excludes = [
            RequestMapping::class,
            GetMapping::class,
            PostMapping::class,
            PutMapping::class,
            PatchMapping::class,
            DeleteMapping::class,
            JwtAuth::class,
            Validate::class,
            ParamInject::class
        ];

        $extraAnnotations = [];

        foreach ($annos as $anno) {
            if (!is_object($anno) || !method_exists($anno, 'getValue')) {
                continue;
            }

            $clazz = StringUtils::ensureLeft(get_class($anno), "\\");
            $isExclude = false;

            foreach ($excludes as $s1) {
                if (strpos($clazz, $s1) !== false) {
                    $isExclude = true;
                    break;
                }
            }

            if ($isExclude) {
                continue;
            }

            $extraAnnotations[] = [
                'annoClazz' => $clazz,
                'initParams' => $anno->getValue()
            ];
        }

        return compact('extraAnnotations');
    }
}
