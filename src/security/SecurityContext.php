<?php

namespace mgboot\security;

use mgboot\common\util\ArrayUtils;

final class SecurityContext
{
    /**
     * @var CorsSettings|null
     */
    private static $corsSettings = null;

    /**
     * @var array
     */
    private static $jwtSettings = [];

    private function __construct()
    {
    }

    /**
     * @param CorsSettings|array $settings
     */
    public static function withCorsSettings($settings): void
    {
        if ($settings instanceof CorsSettings) {
            self::$corsSettings = $settings;
            return;
        }

        if (is_array($settings) && !empty($settings) && ArrayUtils::isAssocArray($settings)) {
            self::$corsSettings = CorsSettings::create($settings);
        }
    }

    public static function getCorsSettings(): ?CorsSettings
    {
        $settings = self::$corsSettings;
        return $settings instanceof CorsSettings ? $settings : null;
    }

    /**
     * @param string $key
     * @param JwtSettings|array $settings
     */
    public static function withJwtSettings(string $key, $settings): void
    {
        $idx = -1;

        foreach (self::$jwtSettings as $i => $st) {
            if (!($st instanceof JwtSettings)) {
                continue;
            }

            if ($st->getKey() === $key) {
                $idx = $i;
                break;
            }
        }

        if ($settings instanceof JwtSettings) {
            if ($idx >= 0) {
                self::$jwtSettings[$idx] = $settings;
            } else {
                self::$corsSettings[] = $settings;
            }

            return;
        }

        if (is_array($settings) && !empty($settings) && ArrayUtils::isAssocArray($settings)) {
            if ($idx >= 0) {
                self::$jwtSettings[$idx] = CorsSettings::create($settings);
            } else {
                self::$corsSettings[] = CorsSettings::create($settings);
            }
        }
    }

    public static function getJwtSettings(string $key): ?JwtSettings
    {
        $matched = null;

        foreach (self::$jwtSettings as $st) {
            if (!($st instanceof JwtSettings)) {
                continue;
            }

            if ($st->getKey() === $key) {
                $matched = $st;
                break;
            }
        }

        return $matched instanceof JwtSettings ? $matched : null;
    }
}
