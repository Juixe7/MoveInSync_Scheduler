package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.repository.PartitionLeaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class PartitionRebalancer {

    private final LeaseManager leaseManager;
    private final PartitionLeaseRepository leaseRepository;
    private final SchedulerProperties properties;

    // Callback: when we acquire new partitions, run misfire scan on them
    private MisfireScanner misfireScanner;

    private ScheduledExecutorService rebalanceThread;
    private volatile boolean running = false;

    public void setMisfireScanner(MisfireScanner scanner) {
        this.misfireScanner = scanner;
    }

    public void start() {
        running = true;
        rebalanceThread = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "partition-rebalancer");
            t.setDaemon(true);
            return t;
        });

        long intervalMs = properties.getRebalanceCooldownSec() * 1000L;
        rebalanceThread.scheduleWithFixedDelay(
            this::rebalance,
            intervalMs,   // initial delay: wait one cooldown period
            intervalMs,
            TimeUnit.MILLISECONDS
        );

        log.info("PartitionRebalancer started, interval={}s", properties.getRebalanceCooldownSec());
    }

    private void rebalance() {
        if (!running) return;

        try {
            // Add jitter
            long jitter = ThreadLocalRandom.current().nextLong(-2000, 2000);
            if (jitter > 0) Thread.sleep(jitter);

            int activeNodes = leaseRepository.countActiveNodes();
            if (activeNodes == 0) activeNodes = 1;

            int fairShare = (int) Math.ceil(
                (double) properties.getPartitionCount() / activeNodes
            );
            int myCount = leaseManager.getOwnedCount();

            log.debug("Rebalance check: activeNodes={}, fairShare={}, myCount={}",
                activeNodes, fairShare, myCount);

            if (myCount < fairShare) {
                // Under-provisioned: try to claim orphaned partitions
                int canTake = fairShare - myCount;
                List<Integer> newPartitions = leaseManager.claimAdditionalPartitions(canTake);

                // Run misfire scan on newly acquired partitions
                if (!newPartitions.isEmpty() && misfireScanner != null) {
                    log.info("Running misfire scan on {} newly acquired partitions",
                        newPartitions.size());
                    misfireScanner.scanPartitions(newPartitions);
                }

            } else if (myCount > fairShare) {
                // Over-provisioned: voluntarily shed excess (new node must have joined)
                int excess = myCount - fairShare;

                // Release the most recently acquired partitions (least disruption)
                List<Integer> toRelease = selectPartitionsToRelease(excess);
                if (!toRelease.isEmpty()) {
                    leaseManager.releasePartitions(toRelease);
                    log.info("Voluntary shedding: released {} partitions for rebalance, " +
                        "activeNodes={}, fairShare={}", toRelease.size(), activeNodes, fairShare);
                }
            }
            // else: exactly at fair share, do nothing

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error during rebalance", e);
        }
    }

    /**
     * Select which partitions to release when shedding.
     * Strategy: release the highest-numbered partitions we own.
     * (Simple, deterministic. An alternative: release most recently acquired.)
     */
    private List<Integer> selectPartitionsToRelease(int count) {
        List<Integer> owned = new ArrayList<>(leaseManager.getOwnedPartitions());
        owned.sort(Comparator.reverseOrder());  // highest first
        return new ArrayList<>(owned.subList(0, Math.min(count, owned.size())));
    }

    public void shutdown() {
        running = false;
        if (rebalanceThread != null) {
            rebalanceThread.shutdown();
            try {
                rebalanceThread.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

