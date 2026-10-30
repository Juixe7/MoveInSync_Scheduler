package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.repository.PartitionLeaseRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Component
@Slf4j
public class MisfireScanner {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final PartitionLeaseRepository leaseRepository;
    private final CronResolver cronResolver;
    private final SchedulerProperties properties;
    private final com.movesync.scheduler.metrics.SchedulerMetrics metrics;

    @org.springframework.beans.factory.annotation.Autowired
    public MisfireScanner(JobRepository jobRepository,
                          JobExecutionRepository executionRepository,
                          PartitionLeaseRepository leaseRepository,
                          CronResolver cronResolver,
                          SchedulerProperties properties,
                          com.movesync.scheduler.metrics.SchedulerMetrics metrics) {
        this.jobRepository = jobRepository;
        this.executionRepository = executionRepository;
        this.leaseRepository = leaseRepository;
        this.cronResolver = cronResolver;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Scan a set of newly acquired partitions for misfired jobs.
     * Called by PartitionRebalancer after claiming orphaned partitions.
     */
    public void scanPartitions(List<Integer> partitionIds) {
        log.info("Misfire scan starting for partitions: {}", partitionIds);
        int handled = 0;

        for (int partitionId : partitionIds) {
            handled += scanPartition(partitionId);
        }

        log.info("Misfire scan complete: {} jobs handled across {} partitions",
            handled, partitionIds.size());
    }

    @Transactional
    public int scanPartition(int partitionId) {
        int handled = 0;

        // 1. Find stuck RUNNING executions from dead nodes
        handled += recoverStuckExecutions(partitionId);

        // 2. Find misfired jobs (fire time in the past, not yet executed)
        handled += handleMisfiredJobs(partitionId);

        return handled;
    }

    /**
     * Find executions stuck in RUNNING from nodes that are no longer alive.
     * Mark them FAILED so they can be retried by the normal scheduling loop.
     */
    private int recoverStuckExecutions(int partitionId) {
        // Find RUNNING executions for this partition
        List<JobExecution> stuckExecutions = executionRepository
            .findStuckByPartition(partitionId);

        int recovered = 0;
        for (JobExecution exec : stuckExecutions) {
            // Is the owning node still alive?
            boolean nodeAlive = leaseRepository.hasValidLeases(exec.getNodeId());

            if (!nodeAlive) {
                // Node is dead. Mark execution as failed for retry.
                executionRepository.updateForRetry(
                    exec.getId(),
                    ExecutionStatus.FAILED,
                    exec.getAttemptNumber(),
                    "OWNER_NODE_DEAD:" + exec.getNodeId()
                );
                recovered++;
                log.warn("Recovered stuck execution: execId={}, jobId={}, deadNode={}",
                    exec.getId(), exec.getJobId(), exec.getNodeId());
            }
            // If node is alive, leave it alone — it might still be working
        }

        return recovered;
    }

    /**
     * Find jobs whose next_fire_time is in the past but have no execution
     * for that fire time. Apply each job's misfire policy.
     */
    private int handleMisfiredJobs(int partitionId) {
        Instant lookbackLimit = Instant.now().minus(
            properties.getMaxMisfireLookbackHours(), ChronoUnit.HOURS);

        // Find jobs that are overdue
        List<Job> overdueJobs = jobRepository.findOverdueByPartition(
            partitionId, lookbackLimit);

        int handled = 0;
        for (Job job : overdueJobs) {
            // Check if there's already an execution for this fire time
            String idempotencyKey = JobExecution.buildIdempotencyKey(
                job.getId(), job.getNextFireTime());
            if (executionRepository.existsByIdempotencyKey(idempotencyKey)) {
                continue;  // Already handled
            }

            handled += applyMisfirePolicy(job);
        }

        return handled;
    }

    private int applyMisfirePolicy(Job job) {
        if (job.getMisfirePolicy() != null) {
            metrics.incrementMisfireRecovered(job.getTenantId(), job.getMisfirePolicy().name());
        }
        switch (job.getMisfirePolicy()) {
            case FIRE_NOW:
                // Leave the job as-is. Its next_fire_time is in the past,
                // so the normal poll loop will pick it up immediately.
                log.info("MISFIRE_FIRE_NOW: jobId={}, overdue since {}",
                    job.getId(), job.getNextFireTime());
                return 1;

            case SKIP:
                // Advance to next fire time (recurring) or mark complete (one-off)
                if (job.getScheduleType() == ScheduleType.RECURRING) {
                    Instant nextFire = cronResolver.nextFireTime(
                        job.getCronExpression(),
                        job.getTimezone() != null ? job.getTimezone() : "UTC",
                        Instant.now()  // next fire AFTER now, not after the missed time
                    );
                    jobRepository.updateNextFireTime(job.getId(), nextFire, Instant.now());
                    log.info("MISFIRE_SKIP: jobId={}, skipped to next fire at {}",
                        job.getId(), nextFire);
                } else {
                    jobRepository.updateStatus(job.getId(), JobStatus.COMPLETED, Instant.now());
                    log.info("MISFIRE_SKIP: jobId={} (one-off), marked COMPLETED",
                        job.getId());
                }
                return 1;

            case COALESCE:
                // For recurring: skip all missed fires, run once with latest fire time
                // Then advance to next future fire time
                if (job.getScheduleType() == ScheduleType.RECURRING) {
                    // Count how many fires were missed
                    int missedCount = countMissedFires(job);

                    // Leave current next_fire_time as-is (which is the oldest missed one)
                    // The scheduler will fire it once, and the handler will see the
                    // coalesce info in the execution metadata
                    log.info("MISFIRE_COALESCE: jobId={}, {} fires coalesced into one",
                        job.getId(), missedCount);
                } else {
                    // One-off: same as FIRE_NOW
                    log.info("MISFIRE_COALESCE: jobId={} (one-off), firing now", job.getId());
                }
                return 1;

            default:
                log.warn("Unknown misfire policy {} for job {}", job.getMisfirePolicy(), job.getId());
                return 0;
        }
    }

    /**
     * Count how many cron fires were missed between the stored next_fire_time and now.
     */
    private int countMissedFires(Job job) {
        Instant current = job.getNextFireTime();
        Instant now = Instant.now();
        int count = 0;
        int maxCount = 1000;  // safety cap

        while (current.isBefore(now) && count < maxCount) {
            current = cronResolver.nextFireTime(
                job.getCronExpression(),
                job.getTimezone() != null ? job.getTimezone() : "UTC",
                current
            );
            count++;
        }

        return count;
    }
}

