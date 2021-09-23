<?php

namespace mgboot\annotation;

use Doctrine\Common\Annotations\Annotation\Target;
use mgboot\common\constant\Regexp;
use mgboot\common\util\StringUtils;

/**
 * @Annotation
 * @Target("CLASS")
 */
class WsTask
{
    /**
     * @var string
     */
    private $value;

    public function __construct($arg0)
    {
        $value = '';

        if (is_string($arg0)) {
            $value = $arg0;
        } else if (is_array($arg0) && is_string($arg0['value'])) {
            $value = $arg0['value'];
        }

        $this->value = $value;
    }

    /**
     * @return string
     */
    public function getValue(): string
    {
        $expr = trim($this->value);

        if ($expr === '') {
            return $expr;
        }

        $expr = preg_replace(Regexp::SPACE_SEP, ' ', $expr);

        if (StringUtils::startsWith($expr, 'every')) {
            return '@' . $expr;
        }

        if (StringUtils::startsWith($expr, '@hourly')) {
            return '@every 1h';
        }

        if (StringUtils::startsWith($expr, '@')) {
            return $expr;
        }

        $parts = explode(' ', $expr);
        $n1 = count($parts);

        if ($n1 < 5) {
            for ($i = 1; $i <= 5 - $n1; $i++) {
                $parts[] = '*';
            }
        }

        $regex1 = '~0/([1-9][0-9]+)~';
        $matches = [];
        preg_match($regex1, $parts[0], $matches, PREG_SET_ORDER);

        if (count($matches) > 1) {
            $n2 = (int) $matches[1];

            if ($n2 > 0) {
                return "@every {$n2}m";
            }
        }

        $matches = [];
        preg_match($regex1, $parts[1], $matches, PREG_SET_ORDER);

        if (count($matches) > 1) {
            $n2 = (int) $matches[1];

            if ($n2 > 0) {
                return "@every {$n2}h";
            }
        }

        return implode(' ', $parts);
    }
}
