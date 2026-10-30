package com.movesync.scheduler.security;

import com.movesync.scheduler.config.SecurityProperties;
import io.jsonwebtoken.Claims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JwtAuthTest {

    private JwtTokenProvider jwtTokenProvider;
    private SecurityProperties securityProperties;

    @BeforeEach
    void setUp() {
        securityProperties = new SecurityProperties();
        jwtTokenProvider = new JwtTokenProvider(securityProperties);
    }

    @Test
    @DisplayName("Should generate valid JWT with tenantId and role claims")
    void shouldGenerateValidToken() {
        UUID tenantId = UUID.randomUUID();
        String token = jwtTokenProvider.generateToken("admin_test", tenantId, "ADMIN");

        assertThat(token).isNotBlank();
        assertThat(jwtTokenProvider.validateToken(token)).isTrue();
        assertThat(jwtTokenProvider.extractUsername(token)).isEqualTo("admin_test");
        assertThat(jwtTokenProvider.extractTenantId(token)).isEqualTo(tenantId);
        assertThat(jwtTokenProvider.extractRole(token)).isEqualTo("ADMIN");
    }

    @Test
    @DisplayName("Should reject tampered or invalid JWT token")
    void shouldRejectInvalidToken() {
        assertThat(jwtTokenProvider.validateToken("invalid.token.structure")).isFalse();
        assertThat(jwtTokenProvider.validateToken("")).isFalse();
    }
}
