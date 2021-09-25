<?php

namespace mgboot\annotation;

/**
 * @Annotation
 */
final class JwtClaim
{
    /**
     * @var string
     */
    private $name;

    public function __construct(?string $name)
    {
        $this->name = empty($name) ? '' : $name;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }
}
