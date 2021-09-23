<?php

namespace mgboot\annotation;

use Doctrine\Common\Annotations\Annotation\Target;
use mgboot\common\Cast;
use mgboot\common\constant\Regexp;

/**
 * @Annotation
 * @Target("METHOD")
 */
final class Validate
{
    /**
     * @var string[]
     */
    private $rules;

    /**
     * @var bool
     */
    private $failfast;

    public function __construct(array $values)
    {
        $rules = [];
        $failfast = Cast::toBoolean($values['failfast']);

        if (is_string($values['rules']) && $values['rules'] !== '') {
            $rules = preg_split(Regexp::COMMA_SEP, $values['rules']);
        } else if (is_array($values['rules']) && !empty($values['rules'])) {
            foreach ($values['rules'] as $s1) {
                if (!is_string($s1) || $s1 === '') {
                    continue;
                }

                $rules[] = $s1;
            }
        }

        $this->rules = $rules;
        $this->failfast = $failfast;
    }

    /**
     * @return string[]
     */
    public function getRules(): array
    {
        return $this->rules;
    }

    /**
     * @return bool
     */
    public function isFailfast(): bool
    {
        return $this->failfast;
    }
}
