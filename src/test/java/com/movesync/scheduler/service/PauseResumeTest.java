package com.movesync.scheduler.service;

import com.movesync.scheduler.domain.model.Tenant;
import com.movesync.scheduler.repository.TenantRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PauseResumeTest {

    @Mock
    private TenantRepository tenantRepository;

    private TenantService tenantService;

    @BeforeEach
    void setUp() {
        tenantService = new TenantService(tenantRepository);
    }

    @Test
    @DisplayName("Caffeine Cache: Lookups are served from cache without extra DB calls")
    void shouldCacheTenantStatusLookups() {
        UUID tenantId = UUID.randomUUID();
        Tenant tenant = Tenant.builder()
            .id(tenantId)
            .name("acme-corp")
            .timezone("Asia/Kolkata")
            .status(Tenant.TenantStatus.ACTIVE)
            .build();

        when(tenantRepository.findById(tenantId)).thenReturn(Optional.of(tenant));

        // First lookup hits repository
        boolean paused1 = tenantService.isTenantPaused(tenantId);
        assertThat(paused1).isFalse();

        // Second and third lookups must hit Caffeine cache
        boolean paused2 = tenantService.isTenantPaused(tenantId);
        boolean paused3 = tenantService.isTenantPaused(tenantId);
        assertThat(paused2).isFalse();
        assertThat(paused3).isFalse();

        // Repository findById called only once
        verify(tenantRepository, times(1)).findById(tenantId);
    }

    @Test
    @DisplayName("Tenant Pause and Resume: Status changes and invalidates cache immediately")
    void shouldPauseAndResumeTenantWithImmediateInvalidation() {
        UUID tenantId = UUID.randomUUID();
        Tenant tenant = Tenant.builder()
            .id(tenantId)
            .name("enterprise-tenant")
            .timezone("UTC")
            .status(Tenant.TenantStatus.ACTIVE)
            .build();

        when(tenantRepository.findById(tenantId)).thenReturn(Optional.of(tenant));
        when(tenantRepository.save(any(Tenant.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // Initial check: active
        assertThat(tenantService.isTenantPaused(tenantId)).isFalse();

        // Pause tenant
        Tenant paused = tenantService.pauseTenant(tenantId);
        assertThat(paused.getStatus()).isEqualTo(Tenant.TenantStatus.PAUSED);
        assertThat(paused.getPausedAt()).isNotNull();

        // Checked immediately: cache is invalidated, returns paused = true
        assertThat(tenantService.isTenantPaused(tenantId)).isTrue();

        // Resume tenant
        Tenant resumed = tenantService.resumeTenant(tenantId);
        assertThat(resumed.getStatus()).isEqualTo(Tenant.TenantStatus.ACTIVE);
        assertThat(resumed.getPausedAt()).isNull();

        // Checked immediately: cache is invalidated, returns paused = false
        assertThat(tenantService.isTenantPaused(tenantId)).isFalse();
    }
}

