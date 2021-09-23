<?php

namespace mgboot\annotation;

use Doctrine\Common\Annotations\Annotation\Target;
use mgboot\common\Cast;
use mgboot\common\constant\RequestParamSecurityMode as SecurityMode;

/**
 * @Annotation
 * @Target("ANNOTATION")
 */
final class RequestParam
{

    /**
     * @var string
     */
    private $name;

    /**
     * @var bool
     */
    private $decimal;

    /**
     * @var int
     */
    private $securityMode;

    public function __construct($arg0 = null)
    {
        $name = '';
        $decimal = false;
        $securityMode = SecurityMode::STRIP_TAGS;

        $modes = [
            SecurityMode::NONE,
            SecurityMode::HTML_PURIFY,
            SecurityMode::STRIP_TAGS
        ];

        if (is_string($arg0) && $arg0 !== '') {
            $name = $arg0;
        } else if (is_array($arg0) && !empty($arg0)) {
            if (is_string($arg0['name']) && $arg0['name'] !== '') {
                $name = $arg0['name'];
            }

            if (is_bool($arg0['decimal'])) {
                $decimal = $arg0['decimal'];
            }

            if (is_int($arg0['securityMode']) && in_array($arg0['securityMode'], $modes)) {
                $securityMode = $arg0['securityMode'];
            } else if (is_string($arg0['securityMode']) && is_numeric($arg0['securityMode'])) {
                $n1 = Cast::toInt($arg0['securityMode']);

                if (in_array($n1, $modes)) {
                    $securityMode = $n1;
                }
            }
        }

        $this->name = $name;
        $this->decimal = $decimal;
        $this->securityMode = $securityMode;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
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
}
