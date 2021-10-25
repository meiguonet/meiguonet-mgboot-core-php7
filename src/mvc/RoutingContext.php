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
use mgboot\common\AppConf;
use mgboot\common\swoole\Swoole;
use mgboot\common\util\FileUtils;
use mgboot\common\util\ReflectUtils;
use mgboot\common\util\StringUtils;
use mgboot\common\util\TokenizeUtils;
use mgboot\exception\HttpError;
use mgboot\http\server\Request;
use mgboot\http\server\Response;
use ReflectionClass;
use ReflectionMethod;
use ReflectionNamedType;
use Throwable;

final class RoutingContext
{
    /**
     * @var array
     */
    private static $map1 = [];

    /**
     * @var Request
     */
    private $request;

    /**
     * @var Response
     */
    private $response;

    /**
     * @var bool
     */
    private $hasNext = true;

    private function __construct(Request $request, Response $response)
    {
        $this->request = $request;
        $this->response = $response;
    }

    public static function create(Request $request, Response $response): RoutingContext
    {
        return new self($request, $response);
    }

    /**
     * @param string|null $scanControllersIn
     * @param string $cacheDir
     */
    public static function buildRouteRules(?string $scanControllersIn = null, string $cacheDir = ''): void
    {
        $inDevMode = AppConf::getEnv() === 'dev';
        $forceCache = AppConf::getBoolean('app.forceRouteRulesCache');

        if ($inDevMode && !$forceCache) {
            return;
        }

        if (is_string($scanControllersIn) && $scanControllersIn !== '') {
            $dir = FileUtils::getRealpath($scanControllersIn);
        } else {
            $dir = FileUtils::getRealpath('classpath:controller');
        }

        $rules = self::buildRouteRulesInternal($dir);

        if (empty($rules)) {
            return;
        }

        if (Swoole::inCoroutineMode(true)) {
            $workerId = Swoole::getWorkerId();
            $key = "routeRulesWorker$workerId";
            self::$map1[$key] = $rules;
            return;
        }

        if ($cacheDir === '') {
            $cacheDir = FileUtils::getRealpath('classpath:cache');
        } else {
            $cacheDir = FileUtils::getRealpath($cacheDir);
        }

        $cacheFile = "$cacheDir/route_routes.php";
        self::writeRouteRulesToCacheFile($cacheFile, $rules);
    }

    /**
     * @param string|null $scanControllersIn
     * @param string $cacheDir
     * @param int|null $workerId
     * @return RouteRule[]
     */
    public static function getRouteRules(?string $scanControllersIn = null, string $cacheDir = '', ?int $workerId = null): array
    {
        if (Swoole::inCoroutineMode(true)) {
            if (!is_int($workerId) || $workerId < 0) {
                $workerId = Swoole::getWorkerId();
            }

            $key = "routeRulesWorker$workerId";
            $rules = self::$map1[$key];
            return is_array($rules) ? $rules : [];
        }

        $inDevMode = AppConf::getEnv() === 'dev';
        $forceCache = AppConf::getBoolean('app.forceRouteRulesCache');

        if ($inDevMode && !$forceCache) {
            if (is_string($scanControllersIn) && $scanControllersIn !== '') {
                $dir = FileUtils::getRealpath($scanControllersIn);
            } else {
                $dir = FileUtils::getRealpath('classpath:controller');
            }

            return self::buildRouteRulesInternal($dir);
        }

        if ($cacheDir === '') {
            $cacheDir = FileUtils::getRealpath('classpath:cache');
        } else {
            $cacheDir = FileUtils::getRealpath($cacheDir);
        }

        $cacheFile = "$cacheDir/route_routes.php";
        $rules = [];

        if (is_file($cacheFile)) {
            try {
                $rules = include($rules);
            } catch (Throwable $ex) {
                $rules = [];
            }
        }

        return array_map(function ($rr) {
            $funcArgs = is_array($rr['handlerFuncArgs']) ? $rr['handlerFuncArgs'] : [];

            $rr['handlerFuncArgs'] = array_map(function ($info) {
                return HandlerFuncArgInfo::create($info);
            }, $funcArgs);

            return RouteRule::create($rr);
        }, $rules);
    }

    public function next(?bool $arg0 = null): bool
    {
        if (is_bool($arg0)) {
            $this->hasNext = $arg0;
            return true;
        }

        return $this->hasNext;
    }

    public function hasError(): bool
    {
        $payload = $this->response->getPayload();
        return $payload instanceof Throwable || $payload instanceof HttpError;
    }

    /**
     * @return Request
     */
    public function getRequest(): Request
    {
        return $this->request;
    }

    /**
     * @return Response
     */
    public function getResponse(): Response
    {
        return $this->response;
    }

    private static function buildRouteRulesInternal(string $dir): array
    {
        if (!is_dir($dir)) {
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
        $map1 = compact('requestMapping');

        if (strpos($requestMapping, ':') === false && strpos($requestMapping, '{') === false) {
            return $map1;
        }

        $sb = [];
        $pathVariableNames = [];
        $parts = explode('/', trim($requestMapping, '/'));

        foreach ($parts as $p) {
            if (StringUtils::startsWith($p, ':')) {
                $sb[] = '([^/]+)';
                $pathVariableNames[] = trim($p, ':');
                continue;
            }

            if (StringUtils::startsWith($p, '{') && StringUtils::endsWith($p, '}')) {
                $sb[] = '([^/]+)';
                $pathVariableNames[] = rtrim(ltrim($p, '{'), '}');
                continue;
            }

            $sb[] = $p;
        }

        if (!empty($pathVariableNames)) {
            $map1['regex'] = sprintf('~^/%s$~', implode('/', $sb));
            $map1['pathVariableNames'] = $pathVariableNames;
        }

        return $map1;
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

            if (is_array($injectRules['value'])) {
                $injectRules = $injectRules['value'];
            }
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

        $extraAnnotations = [];

        foreach ($annos as $anno) {
            if (!is_object($anno) || !method_exists($anno, '__toString')) {
                continue;
            }

            try {
                $contents = $anno->__toString();
            } catch (Throwable $ex) {
                continue;
            }

            if (!is_string($contents) || $contents === '') {
                continue;
            }

            $clazz = get_class($anno);

            if (strpos($contents, $clazz) === false) {
                continue;
            }

            $extraAnnotations[] = $contents;
        }

        return compact('extraAnnotations');
    }

    private static function writeRouteRulesToCacheFile(string $cacheFile, array $rules): void
    {
        if (empty($rules)) {
            return;
        }

        $dir = dirname($cacheFile);

        if (!is_string($dir) || $dir === '') {
            return;
        }

        if (!is_dir($dir)) {
            mkdir($dir, 0644, true);
        }

        if (!is_dir($dir) || !is_writable($dir)) {
            return;
        }

        $fp = fopen("$dir/route_rules.php", 'w');

        if (!is_resource($fp)) {
            return;
        }

        $entries = [];

        /* @var RouteRule $rule */
        foreach ($rules as $rule) {
            $entry = $rule->toMap();

            $entry['handlerFuncArgs'] = array_map(function (HandlerFuncArgInfo $info) {
                return $info->toMap();
            }, $rule->getHandlerFuncArgs());

            $entries[] = $entry;
        }

        $sb = [
            "<?php\n",
            'return ' . var_export($entries, true) . ";\n"
        ];

        flock($fp, LOCK_EX);
        fwrite($fp, implode('', $sb));
        flock($fp, LOCK_UN);
        fclose($fp);
    }
}
