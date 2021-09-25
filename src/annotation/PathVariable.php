<?php

namespace mgboot\annotation;

/**
 * @Annotation
 */
final class PathVariable
{
    /**
     * @var string
     */
    private $name;

    public function __construct(?string $name = null)
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
