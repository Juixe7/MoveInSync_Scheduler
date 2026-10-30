package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.RetryCalculator;
import com.movesync.scheduler.exception.StaleLeaseException;
import com.movesync.scheduler.handler.JobHandler;
import com.movesync.scheduler.handler.JobHandlerRegistry;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.service.TenantService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
public class SchedulerEngine {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final LeaseManager leaseManager;
    private final FencedWriter fencedWriter;
    private final FairnessQueue fairnessQueue;
    private final TenantService tenantService;
    private final JobHandlerRegistry handlerRegistry;
    private final RetryCalculator retryCalculator;
    private final SchedulerProperties properties;
    private final TransactionTemplate transactionTemplate;
    private final com.movesync.scheduler.metrics.SchedulerMetrics metrics;

    public SchedulerEngine(JobRepository jobRepository,
                           JobExecutionRepository executionRepository,
                           LeaseManager leaseManager,
                           FencedWriter fencedWriter,
                           FairnessQueue fairnessQueue,
                           TenantService tenantService,
                           JobHandlerRegistry handlerRegistry,
                           RetryCalculator retryCalculator,
                           SchedulerProperties properties,
                           TransactionTemplate transactionTemplate) {
        this(jobRepository, executionRepository, leaseManager, fencedWriter, fairnessQueue, tenantService,
             handlerRegistry, retryCalculator, properties, transactionTemplate, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public SchedulerEngine(JobRepository jobRepository,
                           JobExecutionRepository executionRepository,
                           LeaseManager leaseManager,
                           FencedWriter fencedWriter,
                           FairnessQueue fairnessQueue,
                           TenantService tenantService,
                           JobHandlerRegistry handlerRegistry,
                           RetryCalculator retryCalculator,
                           SchedulerProperties properties,
                           TransactionTemplate transactionTemplate,
                           @org.springframework.beans.factory.annotation.Autowired(required = false) com.movesync.scheduler.metrics.SchedulerMetrics metrics) {
        this.jobRepository = jobRepository;
        this.executionRepository = executionRepository;
        this.leaseManager = leaseManager;
        this.fencedWriter = fencedWriter;
        this.fairnessQueue = fairnessQueue;
        this.tenantService = tenantService;
        this.handlerRegistry = handlerRegistry;
        this.retryCalculator = retryCalculator;
        this.properties = properties;
        this.transactionTemplate = transactionTemplate;
        this.metrics = metrics;
    }

    private ExecutorService workerPool;
    private ExecutorService criticalPool;
    private ScheduledExecutorService mainLoop;
    private volatile boolean running = false;

    public void start() {
        log.info("Starting scheduler engine, nodeId={}", properties.getNodeId());

        // Initialize thread pools
        workerPool = Executors.newFixedThreadPool(properties.getWorkerPoolSize());
        criticalPool = Executors.newFixedThreadPool(properties.getCriticalPoolSize());
        mainLoop = Executors.newSingleThreadScheduledExecutor();

        // Start the poll loop
        running = true;
        mainLoop.scheduleWithFixedDelay(
            this::pollAndExecute,
            0,
            properties.getPollIntervalMs(),
            TimeUnit.MILLISECONDS
        );

        log.info("Scheduler engine started, nodeId={}", properties.getNodeId());
    }

    private void pollAndExecute() {
        if (!running) return;

        List<Integer> ownedPartitions = leaseManager.getOwnedPartitionList();
        if (ownedPartitions.isEmpty()) return;

        List<Map.Entry<Job, JobExecution>> jobsToDispatch = new ArrayList<>();

        try {
            transactionTemplate.executeWithoutResult(txStatus -> {
                List<Job> dueJobs = jobRepository.findDueJobs(
                    ownedPartitions,
                    properties.getPollBatchSize()
                );

                for (Job job : dueJobs) {
                    // Check 1: Is tenant paused? (uses cache, zero DB hit)
                    if (tenantService.isTenantPaused(job.getTenantId())) {
                        log.debug("Tenant {} is paused, skipping job {}", job.getTenantId(), job.getId());
                        continue;
                    }

                    // Check 2: Still own this partition?
                    if (!leaseManager.ownsPartition(job.getPartitionId())) {
                        log.debug("No longer own partition {}, skipping job {}", job.getPartitionId(), job.getId());
                        continue;
                    }

                    // Check 3: Fairness — tenant at capacity?
                    if (!fairnessQueue.tryAcquire(job.getTenantId())) {
                        log.debug("Tenant {} at concurrency cap, deferring job {}", job.getTenantId(), job.getId());
                        continue;
                    }

                    JobExecution execution = prepareJobExecution(job);
                    if (execution != null) {
                        jobsToDispatch.add(Map.entry(job, execution));
                    } else {
                        fairnessQueue.release(job.getTenantId());
                    }
                }
            });
        } catch (Exception e) {
            log.error("Error in poll loop", e);
            for (Map.Entry<Job, JobExecution> entry : jobsToDispatch) {
                fairnessQueue.release(entry.getKey().getTenantId());
            }
            return;
        }

        // Dispatch outside the transaction so the execution row is committed and visible to worker threads
        for (Map.Entry<Job, JobExecution> entry : jobsToDispatch) {
            Job job = entry.getKey();
            JobExecution execution = entry.getValue();
            ExecutorService pool = (job.getPriority() == Priority.CRITICAL)
                ? criticalPool : workerPool;

            try {
                pool.submit(() -> {
                    try {
                        Integer claimed = transactionTemplate.execute(status ->
                            executionRepository.claimForExecution(execution.getId()));
                        if (claimed == null || claimed == 0) {
                            log.info("Job {} execution was cancelled before firing", job.getId());
                            return;
                        }
                        execution.setStatus(ExecutionStatus.RUNNING);
                        execution.setActualStartTime(Instant.now());
                        execution.computeFireLag();
                        if (metrics != null && execution.getFireLagMs() != null) {
                            metrics.recordFireLag(execution.getFireLagMs());
                        }
                        executeJob(job, execution);
                    } catch (Exception e) {
                        log.error("Error executing job {}", job.getId(), e);
                    } finally {
                        fairnessQueue.release(job.getTenantId());
                    }
                });
            } catch (Exception e) {
                fairnessQueue.release(job.getTenantId());
                log.error("Failed to submit job {} to worker pool", job.getId(), e);
            }
        }
    }

    private JobExecution prepareJobExecution(Job job) {
        try {
            // Get fencing token from LeaseManager cache
            Long fencingToken = leaseManager.getFencingToken(job.getPartitionId());
            if (fencingToken == null) {
                log.warn("No lease for partition {}, skipping job {}",
                    job.getPartitionId(), job.getId());
                return null;
            }

            // Create execution record (idempotency check)
            String idempotencyKey = JobExecution.buildIdempotencyKey(
                job.getId(), job.getNextFireTime());

            Optional<JobExecution> existingOpt = executionRepository.findByIdempotencyKey(idempotencyKey);
            if (existingOpt.isPresent()) {
                JobExecution existing = existingOpt.get();
                if (existing.getStatus() == ExecutionStatus.COMPLETED) {
                    return null;
                }
                if (existing.getStatus() == ExecutionStatus.CANCELLED_BEFORE_EXECUTION
                        || existing.getStatus() == ExecutionStatus.DEAD_LETTERED) {
                    return null;
                }
                // Check if this execution belonged to a crashed node or has a stale fencing token
                if (existing.getFencingToken() < fencingToken) {
                    log.info("Recovering orphaned execution for job {} from dead node {} (token {} -> {})",
                        job.getId(), existing.getNodeId(), existing.getFencingToken(), fencingToken);
                    existing.setNodeId(properties.getNodeId());
                    existing.setFencingToken(fencingToken);
                    existing.setStatus(ExecutionStatus.PENDING);
                    existing.setAttemptNumber(existing.getAttemptNumber() + 1);
                    return executionRepository.save(existing);
                }
                // Still being processed with active fencing token
                log.debug("Job {} already active with current fencing token, skipping", job.getId());
                return null;
            }

            JobExecution execution = JobExecution.builder()
                .jobId(job.getId())
                .tenantId(job.getTenantId())
                .partitionId(job.getPartitionId())
                .idempotencyKey(idempotencyKey)
                .scheduledFireTime(job.getNextFireTime())
                .status(ExecutionStatus.PENDING)
                .fencingToken(fencingToken)
                .nodeId(properties.getNodeId())
                .build();
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

            // Fenced completion via FencedWriter
            fencedWriter.completeWithFencing(job, execution, null);
            if (metrics != null) {
                metrics.incrementJobsFired(job.getTenantId(), job.getJobType().name(), ExecutionStatus.COMPLETED.name());
            }

        } catch (StaleLeaseException sle) {
            log.warn("Execution abandoned due to stale lease: job={} partition={}",
                job.getId(), job.getPartitionId());
        } catch (Exception e) {
            handleFailure(job, execution, e);
        }
    }

    public void handleFailure(Job job, JobExecution execution, Exception e) {
        boolean isNonRetryable = e instanceof com.movesync.scheduler.exception.NonRetryableException;

        if (!isNonRetryable && retryCalculator.shouldRetry(execution.getAttemptNumber(), job.getMaxRetries())) {
            if (metrics != null) {
                metrics.incrementRetries(job.getTenantId());
            }
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

            // Schedule retry
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
            // Dead-letter via FencedWriter
            fencedWriter.deadLetterWithFencing(job, execution, e.getClass().getSimpleName() + ": " + e.getMessage());
            if (metrics != null) {
                metrics.incrementDeadLettered(job.getTenantId(), job.getJobType().name());
            }
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
        log.info("Scheduler engine stopped");
    }
}
