package com.movesync.scheduler.security;

import com.movesync.scheduler.config.SecurityProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.util.Date;
import java.util.UUID;

@Component
@Slf4j
public class JwtTokenProvider {

    private final SecretKey secretKey;
    private final long validityInMilliseconds;

    public JwtTokenProvider(SecurityProperties securityProperties) {
        byte[] keyBytes = Decoders.BASE64.decode(securityProperties.getSecret());
        this.secretKey = Keys.hmacShaKeyFor(keyBytes);
        this.validityInMilliseconds = securityProperties.getExpirySeconds() * 1000L;
    }

    /**
     * Generate a signed JWT token containing subject, tenantId, and role.
     */
    public String generateToken(String username, UUID tenantId, String role) {
        Date now = new Date();
        Date validity = new Date(now.getTime() + validityInMilliseconds);

        return Jwts.builder()
            .subject(username)
            .claim("tenantId", tenantId.toString())
            .claim("role", role)
            .issuedAt(now)
            .expiration(validity)
            .signWith(secretKey)
            .compact();
    }

    /**
     * Validate token signature and expiration.
     */
    public boolean validateToken(String token) {
        try {
            Jwts.parser()
                .verifyWith(secretKey)
                .build()
                .parseSignedClaims(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            log.warn("Invalid JWT token: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Extract claims payload.
     */
    public Claims extractClaims(String token) {
        return Jwts.parser()
            .verifyWith(secretKey)
            .build()
            .parseSignedClaims(token)
            .getPayload();
    }

    public String extractUsername(String token) {
        return extractClaims(token).getSubject();
    }

    public UUID extractTenantId(String token) {
        String tenantStr = extractClaims(token).get("tenantId", String.class);
        return UUID.fromString(tenantStr);
    }

    public String extractRole(String token) {
        return extractClaims(token).get("role", String.class);
    }
}

