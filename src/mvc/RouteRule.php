<?php

namespace mgboot\mvc;


use mgboot\common\traits\MapAbleTrait;

final class RouteRule
{
    use MapAbleTrait;

    /**
     * @var string
     */
    private $httpMethod = 'GET';

    /**
     * @var string
     */
    private $requestMapping = '';

    /**
     * @var string
     */
    private $handler = '';

    /**
     * @var string
     */
    private $jwtSettingsKey = '';

    /**
     * @var string[]
     */
    private $validateRules = [];

    /**
     * @var bool
     */
    private $failfast = false;

    /**
     * @var string[]
     */
    private $extraAnnotations = [];

    /**
     * @var HandlerFuncArgInfo[]
     */
    private $handlerFuncArgs = [];

    private function __construct(?array $data = null)
    {
        if (empty($data)) {
            return;
        }

        $this->fromMap($data);
    }

    public static function create(?array $data = null): RouteRule
    {
        return new self($data);
    }

    /**
     * @return string
     */
    public function getHttpMethod(): string
    {
        return $this->httpMethod;
    }

    /**
     * @param string $httpMethod
     * @return RouteRule
     */
    public function setHttpMethod(string $httpMethod): RouteRule
    {
        $this->httpMethod = $httpMethod;
        return $this;
    }

    /**
     * @return string
     */
    public function getRequestMapping(): string
    {
        return $this->requestMapping;
    }

    /**
     * @param string $requestMapping
     * @return RouteRule
     */
    public function setRequestMapping(string $requestMapping): RouteRule
    {
        $this->requestMapping = $requestMapping;
        return $this;
    }

    /**
     * @return string
     */
    public function getHandler(): string
    {
        return $this->handler;
    }

    /**
     * @param string $handler
     * @return RouteRule
     */
    public function setHandler(string $handler): RouteRule
    {
        $this->handler = $handler;
        return $this;
    }

    /**
     * @return string
     */
    public function getJwtSettingsKey(): string
    {
        return $this->jwtSettingsKey;
    }

    /**
     * @param string $jwtSettingsKey
     * @return RouteRule
     */
    public function setJwtSettingsKey(string $jwtSettingsKey): RouteRule
    {
        $this->jwtSettingsKey = $jwtSettingsKey;
        return $this;
    }

    /**
     * @return string[]
     */
    public function getValidateRules(): array
    {
        return $this->validateRules;
    }

    /**
     * @param array $validateRules
     * @return RouteRule
     */
    public function setValidateRules(array $validateRules): RouteRule
    {
        $this->validateRules = $validateRules;
        return $this;
    }

    /**
     * @return bool
     */
    public function isFailfast(): bool
    {
        return $this->failfast;
    }

    /**
     * @param bool $failfast
     * @return RouteRule
     */
    public function setFailfast(bool $failfast): RouteRule
    {
        $this->failfast = $failfast;
        return $this;
    }

    /**
     * @return string[]
     */
    public function getExtraAnnotations(): array
    {
        return $this->extraAnnotations;
    }

    /**
     * @param string[] $extraAnnotations
     * @return RouteRule
     */
    public function setExtraAnnotations(array $extraAnnotations): RouteRule
    {
        $this->extraAnnotations = $extraAnnotations;
        return $this;
    }

    /**
     * @return HandlerFuncArgInfo[]
     */
    public function getHandlerFuncArgs(): array
    {
        return $this->handlerFuncArgs;
    }

    /**
     * @param HandlerFuncArgInfo[] $handlerFuncArgs
     * @return RouteRule
     */
    public function setHandlerFuncArgs(array $handlerFuncArgs): RouteRule
    {
        $this->handlerFuncArgs = $handlerFuncArgs;
        return $this;
    }
}
