<?php

namespace mgboot\http\middleware;

use Lcobucci\JWT\Token;
use mgboot\common\util\JwtUtils;
use mgboot\exception\AccessTokenExpiredException;
use mgboot\exception\AccessTokenInvalidException;
use mgboot\exception\RequireAccessTokenException;
use mgboot\mvc\RoutingContext;
use mgboot\security\JwtSettings;
use mgboot\security\SecurityContext;

class JwtAuthMiddleware implements Middleware
{
    private function __construct()
    {
    }

    public static function create(): self
    {
        return new self();
    }

    public function getType(): int
    {
        return Middleware::PRE_HANDLE_MIDDLEWARE;
    }

    public function getOrder(): int
    {
        return Middleware::HIGHEST_ORDER;
    }

    public function preHandle(RoutingContext $ctx): void
    {
        if (!$ctx->next()) {
            return;
        }

        $req = $ctx->getRequest();
        $key = $req->getRouteRule()->getJwtSettingsKey();

        if ($key === '') {
            return;
        }

        $settings = SecurityContext::getJwtSettings($key);

        if (!($settings instanceof JwtSettings) || $settings->getIssuer() === '') {
            return;
        }

        $jwt = $ctx->getRequest()->getJwt();

        if (!($jwt instanceof Token)) {
            $ctx->getResponse()->withPayload(new RequireAccessTokenException());
            $ctx->next(false);
            return;
        }

        list($passed, $errCode) = JwtUtils::verify($jwt, $settings->getIssuer());

        if (!$passed) {
            switch ($errCode) {
                case -1:
                    $ex = new AccessTokenInvalidException();
                    break;
                case -2:
                    $ex = new AccessTokenExpiredException();
                    break;
                default:
                    $ex = null;
            }

            $ctx->getResponse()->withPayload($ex);
            $ctx->next(false);
        }
    }

    public function postHandle(RoutingContext $ctx): void
    {
    }
}
