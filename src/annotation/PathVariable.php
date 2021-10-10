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

    public function __construct($arg0 = null)
    {
        $name = '';

        if (is_string($arg0)) {
            $name = $arg0;
        } else if (is_array($arg0) && is_string($arg0['name'])) {
            $name = $arg0['name'];
        }

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
