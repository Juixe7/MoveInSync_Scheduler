package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class NodeLifecycle {

    private final LeaseManager leaseManager;
    private final PartitionRebalancer rebalancer;
    private final MisfireScanner misfireScanner;
    private final SchedulerEngine schedulerEngine;
    private final SchedulerProperties properties;

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        log.info("=== Node {} starting ===", properties.getNodeId());

        // 1. Initialize lease management (claims partitions)
        leaseManager.initialize();

        // 2. Wire misfire scanner into rebalancer
        rebalancer.setMisfireScanner(misfireScanner);

        // 3. Run initial misfire scan on claimed partitions
        if (!leaseManager.getOwnedPartitions().isEmpty()) {
            misfireScanner.scanPartitions(leaseManager.getOwnedPartitionList());
        }

        // 4. Start the rebalancer (periodic check for orphaned/excess partitions)
        rebalancer.start();

        // 5. Start the scheduling engine (poll loop)
        schedulerEngine.start();

        log.info("=== Node {} fully started, owning {} partitions ===",
            properties.getNodeId(), leaseManager.getOwnedCount());
    }

    @PreDestroy
    public void onShutdown() {
        log.info("=== Node {} shutting down ===", properties.getNodeId());

        // Order matters: stop accepting new work first, then release leases

        // 1. Stop the scheduling engine (finishes in-flight jobs)
        schedulerEngine.stop();

        // 2. Stop the rebalancer
        rebalancer.shutdown();

        // 3. Release all leases (so other nodes can grab them immediately,
        //    instead of waiting 30s for expiry)
        leaseManager.shutdown();

        log.info("=== Node {} shutdown complete ===", properties.getNodeId());
    }
}
