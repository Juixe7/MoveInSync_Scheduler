package com.movesync.scheduler.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "security.jwt")
@Getter
@Setter
public class SecurityProperties {

    /**
     * Base64-encoded HMAC-SHA256 secret key (at least 256 bits).
     */
    private String secret = "cGxlYXNlQ2hhbmdlVGhpc0luUHJvZHVjdGlvbkFTQVBTZWN1cmVLZXkxMjM0NTY=";

    /**
     * Token validity in seconds. Defaults to 24 hours (86400s) for demo/evaluation ease.
     */
    private long expirySeconds = 86400;
}

