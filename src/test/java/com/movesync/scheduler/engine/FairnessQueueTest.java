package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FairnessQueueTest {

    private FairnessQueue fairnessQueue;
    private SchedulerProperties properties;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setMaxConcurrentPerTenant(3); // Small cap for testing
        fairnessQueue = new FairnessQueue(properties);
    }

    @Test
    @DisplayName("G8 Guarantee: Tenant cannot exceed max concurrency cap")
    void shouldEnforceTenantConcurrencyCap() {
        UUID tenantA = UUID.randomUUID();

        // Acquire up to cap
        assertThat(fairnessQueue.tryAcquire(tenantA)).isTrue();
        assertThat(fairnessQueue.tryAcquire(tenantA)).isTrue();
        assertThat(fairnessQueue.tryAcquire(tenantA)).isTrue();

        // 4th acquire must fail (tenant at capacity)
        assertThat(fairnessQueue.tryAcquire(tenantA)).isFalse();
        assertThat(fairnessQueue.getActiveCount(tenantA)).isEqualTo(3);
        assertThat(fairnessQueue.getAvailableSlots(tenantA)).isEqualTo(0);

        // Release one slot
        fairnessQueue.release(tenantA);
        assertThat(fairnessQueue.getActiveCount(tenantA)).isEqualTo(2);
        assertThat(fairnessQueue.getAvailableSlots(tenantA)).isEqualTo(1);

        // Now acquire succeeds again
        assertThat(fairnessQueue.tryAcquire(tenantA)).isTrue();
    }

    @Test
    @DisplayName("G8 Guarantee: One tenant at capacity does not starve other tenants (isolation)")
    void shouldIsolateTenantsConcurrently() {
        UUID tenantA = UUID.randomUUID();
        UUID tenantB = UUID.randomUUID();

        // Max out tenant A
        for (int i = 0; i < 3; i++) {
            assertThat(fairnessQueue.tryAcquire(tenantA)).isTrue();
        }
        assertThat(fairnessQueue.tryAcquire(tenantA)).isFalse();

        // Tenant B has full capacity available
        assertThat(fairnessQueue.tryAcquire(tenantB)).isTrue();
        assertThat(fairnessQueue.tryAcquire(tenantB)).isTrue();
        assertThat(fairnessQueue.getActiveCount(tenantB)).isEqualTo(2);
        assertThat(fairnessQueue.getTotalActiveCount()).isEqualTo(5);
    }
}

