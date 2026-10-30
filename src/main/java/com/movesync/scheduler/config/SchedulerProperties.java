package com.movesync.scheduler.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "scheduler")
@Getter @Setter
public class SchedulerProperties {

    private String nodeId = "node-1";

    // Partition
    private int partitionCount = 64;
    private long pollIntervalMs = 500;
    private int pollBatchSize = 50;

    // Lease
    private int leaseTimeoutSec = 30;
    private int leaseRenewalIntervalSec = 10;
    private int rebalanceCooldownSec = 15;

    // Worker pools
    private int criticalPoolSize = 4;
    private int workerPoolSize = 20;

    // Fairness
    private int maxConcurrentPerTenant = 50;
    private int tenantQueueCapacity = 10000;
    private int fairnessMaxDelaySec = 5;

    // Retry
    private int defaultMaxRetries = 3;
    private long initialBackoffMs = 1000;
    private long maxBackoffMs = 60000;
    private double backoffMultiplier = 2.0;
    private double jitterFactor = 0.25;

    // Misfire
    private int misfireThresholdSec = 30;
    private int maxMisfireLookbackHours = 24;

    // Starvation
    private int maxStarvationDelaySec = 300;

    // Outbox
    private long outboxPollIntervalMs = 1000;
}

