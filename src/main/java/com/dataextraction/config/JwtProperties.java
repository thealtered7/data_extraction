package com.dataextraction.config;

import java.time.Duration;

import com.dataextraction.datapipelines.JwtTokenProvider;
import com.dataextraction.observability.Observability;

public class JwtProperties {
    private static final String JWT_SECRET_ENV_VAR = "DATAPIPELINES_JWT_SECRET_KEY";
    
    private boolean enabled;
    private int ttlSeconds = 5 * 60;

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setTtlSeconds(int ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public int getTtlSeconds() {
        return ttlSeconds;
    }

    public Duration getJwtTokenTtl() {
        return Duration.ofSeconds(ttlSeconds);
    }

    private String validateAndGetSecret() {
        String secret = System.getenv(JWT_SECRET_ENV_VAR);
        if (isEnabled() && (secret == null || secret.isBlank())) {
            throw new IllegalStateException(
                    "datapipelines.jwt.enabled=true but " + JWT_SECRET_ENV_VAR + " is not set");
        }
        return secret;
    }
    public JwtTokenProvider buildJwtTokenProvider() {
        String secret = validateAndGetSecret();
        Duration ttl = getJwtTokenTtl();
        return new JwtTokenProvider(secret, Observability.SERVICE_NAME, ttl, isEnabled());
    }
}
