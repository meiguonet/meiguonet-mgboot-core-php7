<?php

namespace mgboot\annotation;

use Doctrine\Common\Annotations\Annotation\Target;

/**
 * @Annotation
 * @Target("METHOD")
 */
final class ParamInject
{
    /**
     * @var array
     */
    private $value;

    public function __construct(array $values)
    {
        $this->value = $values;
    }

    /**
     * @return array
     */
    public function getValue(): array
    {
        return $this->value;
    }
}
