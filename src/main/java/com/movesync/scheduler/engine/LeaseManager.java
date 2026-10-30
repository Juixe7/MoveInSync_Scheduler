package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.PartitionLease;
import com.movesync.scheduler.repository.PartitionLeaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
public class LeaseManager {

    private final PartitionLeaseRepository leaseRepository;
    private final SchedulerProperties properties;
    private final TransactionTemplate transactionTemplate;
    private final com.movesync.scheduler.metrics.SchedulerMetrics metrics;

    public LeaseManager(PartitionLeaseRepository leaseRepository,
                        SchedulerProperties properties,
                        TransactionTemplate transactionTemplate) {
        this(leaseRepository, properties, transactionTemplate, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public LeaseManager(PartitionLeaseRepository leaseRepository,
                        SchedulerProperties properties,
                        TransactionTemplate transactionTemplate,
                        @org.springframework.beans.factory.annotation.Autowired(required = false) com.movesync.scheduler.metrics.SchedulerMetrics metrics) {
        this.leaseRepository = leaseRepository;
        this.properties = properties;
        this.transactionTemplate = transactionTemplate;
        this.metrics = metrics;
    }

    // In-memory view of what we own — refreshed from DB on every renewal
    private final Set<Integer> ownedPartitions = ConcurrentHashMap.newKeySet();

    // Fencing tokens per partition — read from DB on acquisition
    private final ConcurrentHashMap<Integer, Long> fencingTokens = new ConcurrentHashMap<>();

    private ScheduledExecutorService renewalThread;
    private volatile boolean running = false;

    /**
     * Called on startup. Claims a fair share of partitions.
     */
    @Transactional
    public void initialize() {
        leaseRepository.heartbeatNode(properties.getNodeId());
        int fairShare = computeFairShare();
        log.info("Initializing LeaseManager, nodeId={}, targetPartitions={}",
            properties.getNodeId(), fairShare);

        int claimed = leaseRepository.claimPartitions(
            properties.getNodeId(),
            fairShare,
            properties.getLeaseTimeoutSec()
        );

        refreshOwnedPartitions();
        if (metrics != null) {
            metrics.registerLeasesOwnedGauge(ownedPartitions::size);
        }
        log.info("Claimed {} partitions: {}", claimed, ownedPartitions);

        // Start the renewal loop
        running = true;
        renewalThread = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lease-renewal");
            t.setDaemon(true);
            return t;
        });

        // Add jitter to prevent thundering herd
        long intervalMs = properties.getLeaseRenewalIntervalSec() * 1000L;
        renewalThread.scheduleWithFixedDelay(
            this::renewAndCheck,
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS
        );
    }

    /**
     * Renewal loop: extend leases, detect stolen partitions, refresh state.
     */
    private void renewAndCheck() {
        if (!running) return;

        try {
            // Add jitter (±1.5s) to prevent synchronized renewals across nodes
            long jitter = ThreadLocalRandom.current().nextLong(-1500, 1500);
            if (jitter > 0) Thread.sleep(jitter);

            // Renew all our leases using DB clock inside a transaction
            transactionTemplate.executeWithoutResult(status -> {
                leaseRepository.heartbeatNode(properties.getNodeId());
                int renewed = leaseRepository.renewLeases(
                    properties.getNodeId(),
                    properties.getLeaseTimeoutSec()
                );

                Set<Integer> previouslyOwned = new HashSet<>(ownedPartitions);
                refreshOwnedPartitions();

                // Detect stolen partitions (lease renewal returned fewer than expected)
                Set<Integer> stolen = new HashSet<>(previouslyOwned);
                stolen.removeAll(ownedPartitions);
                if (!stolen.isEmpty()) {
                    log.warn("LEASE_STOLEN: partitions {} were taken by another node", stolen);
                    stolen.forEach(fencingTokens::remove);
                    // SchedulerEngine will check ownedPartitions on next poll
                    // and stop processing jobs from stolen partitions
                }

                log.debug("Lease renewal: renewed={}, owned={}", renewed, ownedPartitions.size());
            });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error during lease renewal", e);
        }
    }

    /**
     * Refresh owned partitions and fencing tokens from DB.
     */
    private void refreshOwnedPartitions() {
        List<PartitionLease> leases = leaseRepository.findByActiveNode(properties.getNodeId());

        ownedPartitions.clear();
        fencingTokens.clear();

        for (PartitionLease lease : leases) {
            ownedPartitions.add(lease.getPartitionId());
            fencingTokens.put(lease.getPartitionId(), lease.getFencingToken());
        }
    }

    /**
     * Compute fair share: ceil(totalPartitions / activeNodes).
     * If no other nodes, take everything (startup race).
     */
    private int computeFairShare() {
        int activeNodes = leaseRepository.countActiveNodes();
        if (activeNodes == 0) activeNodes = 1;  // we're the first
        return (int) Math.ceil(
            (double) properties.getPartitionCount() / activeNodes
        );
    }

    // ===== Public accessors for SchedulerEngine and PartitionRebalancer =====

    public Set<Integer> getOwnedPartitions() {
        return Collections.unmodifiableSet(ownedPartitions);
    }

    public List<Integer> getOwnedPartitionList() {
        return new ArrayList<>(ownedPartitions);
    }

    public Long getFencingToken(int partitionId) {
        return fencingTokens.get(partitionId);
    }

    public boolean ownsPartition(int partitionId) {
        return ownedPartitions.contains(partitionId);
    }

    public int getOwnedCount() {
        return ownedPartitions.size();
    }

    /**
     * Release specific partitions (voluntary shedding for rebalance).
     */
    @Transactional
    public void releasePartitions(List<Integer> partitionIds) {
        if (partitionIds.isEmpty()) return;
        transactionTemplate.executeWithoutResult(status -> {
            leaseRepository.releasePartitions(properties.getNodeId(), partitionIds);
        });
        partitionIds.forEach(id -> {
            ownedPartitions.remove(id);
            fencingTokens.remove(id);
        });
        log.info("Voluntarily released partitions: {}", partitionIds);
    }

    /**
     * Release all partitions (graceful shutdown).
     */
    @Transactional
    public void releaseAll() {
        transactionTemplate.executeWithoutResult(status -> {
            leaseRepository.removeNode(properties.getNodeId());
            leaseRepository.releaseAllPartitions(properties.getNodeId());
        });
        ownedPartitions.clear();
        fencingTokens.clear();
        log.info("Released all partitions for graceful shutdown");
    }

    /**
     * Try to claim additional partitions (called by PartitionRebalancer).
     * Returns list of newly acquired partition IDs.
     */
    @Transactional
    public List<Integer> claimAdditionalPartitions(int count) {
        if (count <= 0) return List.of();

        return transactionTemplate.execute(status -> {
            Set<Integer> before = new HashSet<>(ownedPartitions);

            leaseRepository.claimPartitions(
                properties.getNodeId(),
                count,
                properties.getLeaseTimeoutSec()
            );

            refreshOwnedPartitions();

            // Find what's new
            Set<Integer> newPartitions = new HashSet<>(ownedPartitions);
            newPartitions.removeAll(before);

            if (!newPartitions.isEmpty()) {
                log.info("Claimed {} additional partitions: {}", newPartitions.size(), newPartitions);
            }

            return new ArrayList<>(newPartitions);
        });
    }

    public void shutdown() {
        running = false;
        if (renewalThread != null) {
            renewalThread.shutdown();
            try {
                renewalThread.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        releaseAll();
    }
}

