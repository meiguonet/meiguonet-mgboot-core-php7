<?php

namespace mgboot\mvc;

use mgboot\common\traits\MapAbleTrait;

class HandlerFuncArgInfo
{
    use MapAbleTrait;

    /**
     * @var string
     */
    private $name = '';

    /**
     * @var string
     */
    private $type = '';

    /**
     * @var bool
     */
    private $nullable = false;

    /**
     * @var bool
     */
    private $request = false;

    /**
     * @var bool
     */
    private $jwt = false;

    /**
     * @var bool
     */
    private $clientIp = false;

    /**
     * @var string
     */
    private $httpHeaderName = '';

    /**
     * @var string
     */
    private $jwtClaimName = '';

    /**
     * @var string
     */
    private $pathVariableName = '';

    /**
     * @var string
     */
    private $requestParamName = '';

    /**
     * @var bool
     */
    private $decimal = false;

    /**
     * @var int
     */
    private $securityMode = 3;

    /**
     * @var bool
     */
    private $paramMap = false;

    /**
     * @var array
     */
    private $paramMapRules = [];

    /**
     * @var bool
     */
    private $uploadedFile = false;

    /**
     * @var string
     */
    private $formFieldName = '';

    /**
     * @var bool
     */
    private $needRequestBody = false;

    private function __construct(?array $data = null)
    {
        if (empty($data)) {
            return;
        }

        $this->fromMap($data);
    }

    public static function create(?array $data = null): HandlerFuncArgInfo
    {
        return new self($data);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getType(): string
    {
        return $this->type;
    }

    /**
     * @return bool
     */
    public function isNullable(): bool
    {
        return $this->nullable;
    }

    /**
     * @return bool
     */
    public function isRequest(): bool
    {
        return $this->request;
    }

    /**
     * @return bool
     */
    public function isJwt(): bool
    {
        return $this->jwt;
    }

    /**
     * @return bool
     */
    public function isClientIp(): bool
    {
        return $this->clientIp;
    }

    /**
     * @return string
     */
    public function getHttpHeaderName(): string
    {
        return $this->httpHeaderName;
    }

    /**
     * @return string
     */
    public function getJwtClaimName(): string
    {
        return $this->jwtClaimName;
    }

    /**
     * @return string
     */
    public function getPathVariableName(): string
    {
        return $this->pathVariableName;
    }

    /**
     * @return string
     */
    public function getRequestParamName(): string
    {
        return $this->requestParamName;
    }

    /**
     * @return bool
     */
    public function isDecimal(): bool
    {
        return $this->decimal;
    }

    /**
     * @return int
     */
    public function getSecurityMode(): int
    {
        return $this->securityMode;
    }

    /**
     * @return bool
     */
    public function isParamMap(): bool
    {
        return $this->paramMap;
    }

    /**
     * @return array
     */
    public function getParamMapRules(): array
    {
        return $this->paramMapRules;
    }

    /**
     * @return bool
     */
    public function isUploadedFile(): bool
    {
        return $this->uploadedFile;
    }

    /**
     * @return string
     */
    public function getFormFieldName(): string
    {
        return $this->formFieldName;
    }

    /**
     * @return bool
     */
    public function isNeedRequestBody(): bool
    {
        return $this->needRequestBody;
    }
}
