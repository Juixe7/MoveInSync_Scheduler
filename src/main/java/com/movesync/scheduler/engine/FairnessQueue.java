package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class FairnessQueue {

    private final int maxConcurrentPerTenant;
    private final ConcurrentHashMap<UUID, Semaphore> tenantSemaphores = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, AtomicInteger> tenantActiveCounts = new ConcurrentHashMap<>();

    public FairnessQueue(SchedulerProperties properties) {
        this.maxConcurrentPerTenant = properties.getMaxConcurrentPerTenant();
    }

    /**
     * Try to acquire a slot for this tenant. Non-blocking.
     * @return true if acquired, false if tenant is at capacity on this node
     */
    public boolean tryAcquire(UUID tenantId) {
        Semaphore semaphore = tenantSemaphores.computeIfAbsent(
            tenantId, k -> new Semaphore(maxConcurrentPerTenant));

        boolean acquired = semaphore.tryAcquire();
        if (acquired) {
            tenantActiveCounts
                .computeIfAbsent(tenantId, k -> new AtomicInteger(0))
                .incrementAndGet();
        }
        return acquired;
    }

    /**
     * Release a slot for this tenant. Must be called in finally block.
     */
    public void release(UUID tenantId) {
        Semaphore semaphore = tenantSemaphores.get(tenantId);
        if (semaphore != null) {
            semaphore.release();
            AtomicInteger count = tenantActiveCounts.get(tenantId);
            if (count != null) count.decrementAndGet();
        }
    }

    /**
     * Get current active count for a tenant on this node (for metrics).
     */
    public int getActiveCount(UUID tenantId) {
        AtomicInteger count = tenantActiveCounts.get(tenantId);
        return count != null ? count.get() : 0;
    }

    /**
     * Get total active jobs across all tenants on this node (for metrics).
     */
    public int getTotalActiveCount() {
        return tenantActiveCounts.values().stream()
            .mapToInt(AtomicInteger::get)
            .sum();
    }

    /**
     * Get available slots for a tenant (for debugging/metrics).
     */
    public int getAvailableSlots(UUID tenantId) {
        Semaphore semaphore = tenantSemaphores.get(tenantId);
        return semaphore != null ? semaphore.availablePermits() : maxConcurrentPerTenant;
    }
}

