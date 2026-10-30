package com.movesync.scheduler.service;

import com.movesync.scheduler.domain.model.Tenant;
import com.movesync.scheduler.metrics.SchedulerMetrics;
import com.movesync.scheduler.repository.TenantRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TenantCacheTest {

    @Mock
    private TenantRepository tenantRepository;

    private SchedulerMetrics metrics;
    private TenantService tenantService;

    @BeforeEach
    void setUp() {
        metrics = new SchedulerMetrics(new SimpleMeterRegistry());
        tenantService = new TenantService(tenantRepository, metrics);
    }

    @Test
    @DisplayName("Caffeine cache avoids DB query on repeated isTenantPaused checks within TTL")
    void shouldCacheTenantStatus() {
        UUID tenantId = UUID.randomUUID();
        Tenant tenant = Tenant.builder()
            .id(tenantId)
            .name("Cache-Test-Tenant")
            .status(Tenant.TenantStatus.ACTIVE)
            .build();

        when(tenantRepository.findById(tenantId)).thenReturn(Optional.of(tenant));

        // First call: cache miss, queries DB
        boolean paused1 = tenantService.isTenantPaused(tenantId);
        // Second call: cache hit, no DB query
        boolean paused2 = tenantService.isTenantPaused(tenantId);
        // Third call: cache hit, no DB query
        boolean paused3 = tenantService.isTenantPaused(tenantId);

        assertThat(paused1).isFalse();
        assertThat(paused2).isFalse();
        assertThat(paused3).isFalse();

        // Database should be queried exactly once
        verify(tenantRepository, times(1)).findById(tenantId);

        // Cache stats should record hits
        assertThat(tenantService.getStatusCacheStats().hitCount()).isEqualTo(2);
        assertThat(tenantService.getStatusCacheStats().missCount()).isEqualTo(1);
    }
}

