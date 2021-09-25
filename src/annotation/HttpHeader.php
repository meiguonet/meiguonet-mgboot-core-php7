<?php

namespace mgboot\annotation;

/**
 * @Annotation
 */
final class HttpHeader
{
    /**
     * @var string
     */
    private $name;

    public function __construct(string $arg0)
    {
        $this->name = $arg0;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }
}
