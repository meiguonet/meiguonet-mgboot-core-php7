<?php

namespace mgboot\exception;

use RuntimeException;

final class ValidateException extends RuntimeException
{
    /**
     * @var string
     */
    private $errorTips;

    /**
     * @var array
     */
    private $validateErrors;

    public function __construct(array $validateErrors = [], string $errorTips = '')
    {
        if ($errorTips === '') {
            $errorTips = '数据完整性验证错误';
        }

        parent::__construct($errorTips);
        $this->errorTips = $errorTips;
        $this->validateErrors = $validateErrors;
    }

    /**
     * @return string
     */
    public function getErrorTips(): string
    {
        return $this->errorTips;
    }

    /**
     * @return array
     */
    public function getValidateErrors(): array
    {
        return $this->validateErrors;
    }
}
