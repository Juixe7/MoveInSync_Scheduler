package com.movesync.scheduler.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class TenantIsolationFilter extends OncePerRequestFilter {

    public static final String TENANT_ID_ATTRIBUTE = "tenantId";
    private final JwtTokenProvider jwtTokenProvider;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String token = resolveToken(request);

        if (StringUtils.hasText(token) && jwtTokenProvider.validateToken(token)) {
            try {
                String username = jwtTokenProvider.extractUsername(token);
                UUID tenantId = jwtTokenProvider.extractTenantId(token);
                String role = jwtTokenProvider.extractRole(token);

                String roleName = role.startsWith("ROLE_") ? role : "ROLE_" + role;
                SimpleGrantedAuthority authority = new SimpleGrantedAuthority(roleName);

                UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken(username, null, List.of(authority));

                SecurityContextHolder.getContext().setAuthentication(authentication);

                request.setAttribute(TENANT_ID_ATTRIBUTE, tenantId);
                MDC.put("tenantId", tenantId.toString());

                // Tenant Isolation Check: verify token tenantId matches requested URL path tenantId
                String uri = request.getRequestURI();
                if (uri.startsWith("/api/tenants/")) {
                    String[] parts = uri.split("/");
                    if (parts.length >= 4) {
                        try {
                            UUID pathTenantId = UUID.fromString(parts[3]);
                            if (!pathTenantId.equals(tenantId)) {
                                log.warn("CROSS_TENANT_ACCESS_DENIED: token tenant={} attempted access to path tenant={}",
                                    tenantId, pathTenantId);
                                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                                response.setContentType("application/json");
                                response.getWriter().write(String.format(
                                    "{\"status\":403,\"error\":\"Forbidden\",\"message\":\"Access denied to tenant %s\"}",
                                    pathTenantId
                                ));
                                return;
                            }
                        } catch (IllegalArgumentException ignored) {
                            // Segment is not a UUID, let standard routing handle it
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to set tenant context from JWT: {}", e.getMessage());
            }
        }

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("tenantId");
        }
    }

    private String resolveToken(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);
        }
        return null;
    }
}

