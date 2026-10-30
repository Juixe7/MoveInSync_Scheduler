package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.domain.service.RetryCalculator;
import com.movesync.scheduler.handler.JobHandler;
import com.movesync.scheduler.handler.JobHandlerRegistry;
import com.movesync.scheduler.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class SchedulerEngine {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final PartitionLeaseRepository leaseRepository;
    private final OutboxRepository outboxRepository;
    private final JobHandlerRegistry handlerRegistry;
    private final CronResolver cronResolver;
    private final RetryCalculator retryCalculator;
    private final SchedulerProperties properties;
    private final TransactionTemplate transactionTemplate;

    private ExecutorService workerPool;
    private ExecutorService criticalPool;
    private ScheduledExecutorService mainLoop;
    private volatile boolean running = false;

    // Phase 1: own all partitions (single node)
    // Phase 2 will replace with LeaseManager
    private List<Integer> ownedPartitions;

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("Starting scheduler engine, nodeId={}", properties.getNodeId());

        // Initialize thread pools
        workerPool = Executors.newFixedThreadPool(properties.getWorkerPoolSize());
        criticalPool = Executors.newFixedThreadPool(properties.getCriticalPoolSize());
        mainLoop = Executors.newSingleThreadScheduledExecutor();

        // Phase 1: claim all partitions inside transaction
        transactionTemplate.executeWithoutResult(status -> claimAllPartitions());

        // Start the poll loop
        running = true;
        mainLoop.scheduleWithFixedDelay(
            this::pollAndExecute,
            0,
            properties.getPollIntervalMs(),
            TimeUnit.MILLISECONDS
        );

        log.info("Scheduler engine started, owning {} partitions", ownedPartitions != null ? ownedPartitions.size() : 0);
    }

    @Transactional
    public void claimAllPartitions() {
        // Phase 1: single node claims everything
        int claimed = leaseRepository.claimPartitions(
            properties.getNodeId(),
            properties.getPartitionCount(),   // claim all 64
            properties.getLeaseTimeoutSec()
        );
        ownedPartitions = leaseRepository.findByActiveNode(properties.getNodeId())
            .stream()
            .map(PartitionLease::getPartitionId)
            .toList();
        log.info("Claimed {} partitions", claimed);
    }

    private void pollAndExecute() {
        if (!running || ownedPartitions == null || ownedPartitions.isEmpty()) return;

        List<Map.Entry<Job, JobExecution>> jobsToDispatch = new ArrayList<>();

        try {
            transactionTemplate.executeWithoutResult(txStatus -> {
                // Keep leases fresh in single-node mode until LeaseManager in Phase 2
                leaseRepository.renewLeases(properties.getNodeId(), properties.getLeaseTimeoutSec());

                List<Job> dueJobs = jobRepository.findDueJobs(
                    ownedPartitions,
                    properties.getPollBatchSize()
                );

                for (Job job : dueJobs) {
                    JobExecution execution = prepareJobExecution(job);
                    if (execution != null) {
                        jobsToDispatch.add(Map.entry(job, execution));
                    }
                }
            });
        } catch (Exception e) {
            log.error("Error in poll loop", e);
            return;
        }

        // Dispatch outside the transaction so the execution row is committed and visible to worker threads
        for (Map.Entry<Job, JobExecution> entry : jobsToDispatch) {
            Job job = entry.getKey();
            JobExecution execution = entry.getValue();
            ExecutorService pool = (job.getPriority() == Priority.CRITICAL)
                ? criticalPool : workerPool;

            pool.submit(() -> executeJob(job, execution));
        }
    }

    private JobExecution prepareJobExecution(Job job) {
        try {
            // Get fencing token for this partition
            Long fencingToken = leaseRepository.getFencingToken(
                job.getPartitionId(), properties.getNodeId());
            if (fencingToken == null) {
                log.warn("No lease for partition {}, skipping job {}",
                    job.getPartitionId(), job.getId());
                return null;
            }

            // Create execution record (idempotency check)
            String idempotencyKey = JobExecution.buildIdempotencyKey(
                job.getId(), job.getNextFireTime());

            if (executionRepository.existsByIdempotencyKey(idempotencyKey)) {
                log.debug("Job {} already has execution for this fire time, skipping",
                    job.getId());
                return null;
            }

            JobExecution execution = JobExecution.builder()
                .jobId(job.getId())
                .tenantId(job.getTenantId())
                .partitionId(job.getPartitionId())
                .idempotencyKey(idempotencyKey)
                .scheduledFireTime(job.getNextFireTime())
                .actualStartTime(Instant.now())
                .status(ExecutionStatus.RUNNING)
                .fencingToken(fencingToken)
                .nodeId(properties.getNodeId())
                .build();
            execution.computeFireLag();
            return executionRepository.save(execution);

        } catch (Exception e) {
            log.error("Error preparing job execution {}: {}", job.getId(), e.getMessage());
            return null;
        }
    }

    private void executeJob(Job job, JobExecution execution) {
        try {
            // Get handler
            JobHandler handler = handlerRegistry.getHandler(job.getJobType());

            // Run it
            handler.handle(execution);

            // Fenced completion
            completeExecution(job, execution);

        } catch (Exception e) {
            handleFailure(job, execution, e);
        }
    }

    public void completeExecution(Job job, JobExecution execution) {
        transactionTemplate.executeWithoutResult(txStatus -> {
            // G4: fenced write
            int affected = executionRepository.fencedComplete(
                execution.getId(),
                ExecutionStatus.COMPLETED.name(),
                execution.getFencingToken(),
                null,  // result
                null   // errorMessage
            );

            if (affected == 0) {
                log.warn("FENCING_REJECTED: job={} partition={} token={}",
                    job.getId(), job.getPartitionId(), execution.getFencingToken());
                return; // stale — another node completed this
            }

            // Write outbox event (same transaction)
            outboxRepository.save(OutboxEvent.builder()
                .aggregateType("JOB")
                .aggregateId(job.getId())
                .eventType("JOB_COMPLETED")
                .payload(String.format(
                    "{\"jobId\":\"%s\",\"tenantId\":\"%s\",\"fireLagMs\":%d}",
                    job.getId(), job.getTenantId(),
                    execution.getFireLagMs() != null ? execution.getFireLagMs() : 0))
                .build());

            // Advance next fire time for recurring, or mark complete for one-off
            if (job.getScheduleType() == ScheduleType.RECURRING) {
                Instant nextFire = cronResolver.nextFireTime(
                    job.getCronExpression(),
                    job.getTimezone() != null ? job.getTimezone() : "UTC",
                    job.getNextFireTime());
                jobRepository.updateNextFireTime(job.getId(), nextFire, Instant.now());
            } else {
                jobRepository.updateStatus(job.getId(), JobStatus.COMPLETED, Instant.now());
            }

            log.info("Job completed: id={} type={} fireLag={}ms",
                job.getId(), job.getJobType(),
                execution.getFireLagMs());
        });
    }

    public void handleFailure(Job job, JobExecution execution, Exception e) {
        boolean isNonRetryable = e instanceof com.movesync.scheduler.exception.NonRetryableException;

        if (!isNonRetryable && retryCalculator.shouldRetry(execution.getAttemptNumber(), job.getMaxRetries())) {
            // Retry
            long backoff = retryCalculator.computeBackoffMs(execution.getAttemptNumber());
            transactionTemplate.executeWithoutResult(txStatus -> {
                executionRepository.updateForRetry(
                    execution.getId(),
                    ExecutionStatus.RETRYING,
                    execution.getAttemptNumber() + 1,
                    e.getClass().getSimpleName() + ": " + e.getMessage()
                );
            });

            log.warn("Job {} failed (attempt {}), retrying in {}ms: {}",
                job.getId(), execution.getAttemptNumber(), backoff, e.getMessage());

            // Schedule retry (simplified: sleep then re-execute)
            workerPool.submit(() -> {
                try {
                    Thread.sleep(backoff);
                    execution.setAttemptNumber(execution.getAttemptNumber() + 1);
                    executeJob(job, execution);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        } else {
            // Dead-letter
            transactionTemplate.executeWithoutResult(txStatus -> {
                executionRepository.fencedComplete(
                    execution.getId(),
                    ExecutionStatus.DEAD_LETTERED.name(),
                    execution.getFencingToken(),
                    null,
                    e.getClass().getSimpleName() + ": " + e.getMessage()
                );

                outboxRepository.save(OutboxEvent.builder()
                    .aggregateType("JOB")
                    .aggregateId(job.getId())
                    .eventType("JOB_DEAD_LETTERED")
                    .payload(String.format(
                        "{\"jobId\":\"%s\",\"tenantId\":\"%s\",\"error\":\"%s\"}",
                        job.getId(), job.getTenantId(), e.getMessage()))
                    .build());
            });

            log.error("Job {} dead-lettered after {} attempts: {}",
                job.getId(), execution.getAttemptNumber(), e.getMessage());
        }
    }

    // Graceful shutdown
    public void stop() {
        running = false;
        mainLoop.shutdown();
        workerPool.shutdown();
        criticalPool.shutdown();
        try {
            workerPool.awaitTermination(10, TimeUnit.SECONDS);
            criticalPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transactionTemplate.executeWithoutResult(txStatus -> {
            leaseRepository.releaseAllPartitions(properties.getNodeId());
        });
        log.info("Scheduler engine stopped");
    }
}

