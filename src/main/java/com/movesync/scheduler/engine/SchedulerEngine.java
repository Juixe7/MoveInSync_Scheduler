package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.RetryCalculator;
import com.movesync.scheduler.exception.NonRetryableException;
import com.movesync.scheduler.exception.StaleLeaseException;
import com.movesync.scheduler.handler.JobHandler;
import com.movesync.scheduler.handler.JobHandlerRegistry;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class SchedulerEngine {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final JobHandlerRegistry handlerRegistry;
    private final RetryCalculator retryCalculator;
    private final SchedulerProperties properties;
    private final LeaseManager leaseManager;
    private final FencedWriter fencedWriter;
    private final TransactionTemplate transactionTemplate;

    private ExecutorService workerPool;
    private ExecutorService criticalPool;
    private ScheduledExecutorService mainLoop;
    private volatile boolean running = false;

    /**
     * Called by NodeLifecycle after LeaseManager is initialized.
     */
    public void start() {
        log.info("Starting SchedulerEngine, nodeId={}", properties.getNodeId());

        workerPool = Executors.newFixedThreadPool(properties.getWorkerPoolSize(),
            r -> new Thread(r, "worker-" + r.hashCode()));
        criticalPool = Executors.newFixedThreadPool(properties.getCriticalPoolSize(),
            r -> new Thread(r, "critical-" + r.hashCode()));
        mainLoop = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "scheduler-main"));

        running = true;

        // Add jitter to initial delay to stagger nodes
        long initialDelay = ThreadLocalRandom.current().nextLong(0, properties.getPollIntervalMs());

        mainLoop.scheduleWithFixedDelay(
            this::pollAndExecute,
            initialDelay,
            properties.getPollIntervalMs(),
            TimeUnit.MILLISECONDS
        );

        log.info("SchedulerEngine started");
    }

    private void pollAndExecute() {
        if (!running) return;

        List<Map.Entry<Job, JobExecution>> jobsToDispatch = new ArrayList<>();

        try {
            transactionTemplate.executeWithoutResult(txStatus -> {
                List<Integer> myPartitions = leaseManager.getOwnedPartitionList();
                if (myPartitions.isEmpty()) {
                    return;  // No partitions owned yet (or all stolen)
                }

                List<Job> dueJobs = jobRepository.findDueJobs(
                    myPartitions,
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
            // Double-check we still own this partition
            if (!leaseManager.ownsPartition(job.getPartitionId())) {
                log.debug("No longer own partition {}, skipping job {}",
                    job.getPartitionId(), job.getId());
                return null;
            }

            // Get fencing token from in-memory cache (not DB query per job)
            Long fencingToken = leaseManager.getFencingToken(job.getPartitionId());
            if (fencingToken == null) {
                log.warn("No fencing token for partition {}, skipping job {}",
                    job.getPartitionId(), job.getId());
                return null;
            }

            // Idempotency check
            String idempotencyKey = JobExecution.buildIdempotencyKey(
                job.getId(), job.getNextFireTime());

            if (executionRepository.existsByIdempotencyKey(idempotencyKey)) {
                log.debug("Job {} already has execution, skipping", job.getId());
                return null;
            }

            // Create execution record
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
            JobHandler handler = handlerRegistry.getHandler(job.getJobType());
            handler.handle(execution);

            // Fenced completion (throws StaleLeaseException if fencing fails)
            fencedWriter.completeWithFencing(job, execution, null);

        } catch (StaleLeaseException e) {
            // Fencing rejected — another node took over. Don't retry.
            log.warn("Fencing rejected for job {}, dropping", job.getId());

        } catch (NonRetryableException e) {
            fencedWriter.deadLetterWithFencing(job, execution,
                e.getClass().getSimpleName() + ": " + e.getMessage());

        } catch (Exception e) {
            handleRetry(job, execution, e);
        }
    }

    private void handleRetry(Job job, JobExecution execution, Exception e) {
        if (retryCalculator.shouldRetry(execution.getAttemptNumber(), job.getMaxRetries())) {
            long backoff = retryCalculator.computeBackoffMs(execution.getAttemptNumber());

            transactionTemplate.executeWithoutResult(txStatus -> {
                executionRepository.updateForRetry(
                    execution.getId(),
                    ExecutionStatus.RETRYING,
                    execution.getAttemptNumber() + 1,
                    e.getClass().getSimpleName() + ": " + e.getMessage()
                );
            });

            log.warn("Job {} failed (attempt {}), retrying in {}ms",
                job.getId(), execution.getAttemptNumber(), backoff);

            workerPool.submit(() -> {
                try {
                    Thread.sleep(backoff);
                    // Check we still own the partition before retrying
                    if (!leaseManager.ownsPartition(job.getPartitionId())) {
                        log.warn("Lost partition {} during retry, dropping job {}",
                            job.getPartitionId(), job.getId());
                        return;
                    }
                    execution.setAttemptNumber(execution.getAttemptNumber() + 1);
                    executeJob(job, execution);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
        } else {
            fencedWriter.deadLetterWithFencing(job, execution,
                e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    public void stop() {
        running = false;
        if (mainLoop != null) mainLoop.shutdown();
        if (workerPool != null) {
            workerPool.shutdown();
            try { workerPool.awaitTermination(10, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (criticalPool != null) {
            criticalPool.shutdown();
            try { criticalPool.awaitTermination(10, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        log.info("SchedulerEngine stopped");
    }
}
