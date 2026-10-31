package com.movesync.scheduler.config;

import com.movesync.scheduler.security.TenantIsolationFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final TenantIsolationFilter tenantIsolationFilter;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                // Public docs, health, metrics & Prometheus
                .requestMatchers("/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**", "/actuator/**", "/error").permitAll()

                // RBAC rules for /api/**
                // Tenant Admin actions
                .requestMatchers(HttpMethod.POST, "/api/tenants/*/pause", "/api/tenants/*/resume").hasRole("ADMIN")
                .requestMatchers(HttpMethod.POST, "/api/jobs").hasRole("ADMIN")
                .requestMatchers(HttpMethod.DELETE, "/api/jobs/**").hasRole("ADMIN")

                // Operator & Admin actions (trigger, cancel, replay)
                .requestMatchers(HttpMethod.POST, "/api/jobs/*/trigger").hasAnyRole("ADMIN", "OPERATOR")
                .requestMatchers(HttpMethod.POST, "/api/jobs/*/cancel").hasAnyRole("ADMIN", "OPERATOR")
                .requestMatchers(HttpMethod.POST, "/api/tenants/*/dead-letter/**").hasAnyRole("ADMIN", "OPERATOR")

                // Read-only for all authenticated roles
                .requestMatchers(HttpMethod.GET, "/api/**").hasAnyRole("ADMIN", "OPERATOR", "EMPLOYEE")

                // Any other request requires authentication
                .anyRequest().authenticated()
            )
            .addFilterBefore(tenantIsolationFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
