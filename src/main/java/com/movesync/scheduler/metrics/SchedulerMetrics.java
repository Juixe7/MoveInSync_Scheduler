package com.movesync.scheduler.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

@Component
public class SchedulerMetrics {

    private final MeterRegistry registry;
    private final Timer fireLagTimer;

    public SchedulerMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.fireLagTimer = Timer.builder("scheduler.jobs.fire_lag_ms")
            .description("Scheduled job fire lag in milliseconds")
            .serviceLevelObjectives(Duration.ofMillis(500), Duration.ofMillis(1000), Duration.ofMillis(2000))
            .register(registry);
    }

    public void recordFireLag(long fireLagMs) {
        if (fireLagMs >= 0) {
            fireLagTimer.record(Duration.ofMillis(fireLagMs));
        }
    }

    public void incrementJobsFired(UUID tenantId, String jobType, String status) {
        Counter.builder("scheduler.jobs.fired")
            .tag("tenant", tenantId != null ? tenantId.toString() : "unknown")
            .tag("type", jobType != null ? jobType : "unknown")
            .tag("status", status != null ? status : "unknown")
            .register(registry)
            .increment();
    }

    public void incrementRetries(UUID tenantId) {
        Counter.builder("scheduler.jobs.retried")
            .tag("tenant", tenantId != null ? tenantId.toString() : "unknown")
            .register(registry)
            .increment();
    }

    public void incrementDeadLettered(UUID tenantId, String jobType) {
        Counter.builder("scheduler.jobs.dead_lettered")
            .tag("tenant", tenantId != null ? tenantId.toString() : "unknown")
            .tag("type", jobType != null ? jobType : "unknown")
            .register(registry)
            .increment();
    }

    public void incrementFencingRejected(int partition) {
        Counter.builder("scheduler.fencing.rejected")
            .tag("partition", String.valueOf(partition))
            .register(registry)
            .increment();
    }

    public void incrementMisfireRecovered(UUID tenantId, String policy) {
        Counter.builder("scheduler.jobs.misfire_recovered")
            .tag("tenant", tenantId != null ? tenantId.toString() : "unknown")
            .tag("policy", policy != null ? policy : "unknown")
            .register(registry)
            .increment();
    }

    public void registerLeasesOwnedGauge(Supplier<Number> supplier) {
        Gauge.builder("scheduler.leases.owned", supplier)
            .description("Number of partition leases currently owned by this node")
            .register(registry);
    }

    public void registerCacheHitRateGauge(Supplier<Number> supplier) {
        Gauge.builder("scheduler.cache.tenant_status.hit_rate", supplier)
            .description("Hit rate for tenant status lookup cache")
            .register(registry);
    }
}

