package com.movesync.scheduler.service;

import com.movesync.scheduler.api.dto.*;
import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.domain.service.PartitionAssigner;
import com.movesync.scheduler.engine.LeaseManager;
import com.movesync.scheduler.exception.JobNotFoundException;
import com.movesync.scheduler.exception.SchedulerException;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
    private final JobExecutionRepository executionRepository;
    private final LeaseManager leaseManager;
    private final CronResolver cronResolver;
    private final PartitionAssigner partitionAssigner;
    private final SchedulerProperties properties;

    @Transactional
    public JobResponse createJob(UUID tenantId, CreateJobRequest request) {
        // 1. Idempotent creation check
        if (request.getExternalRefId() != null) {
            Optional<Job> existing = jobRepository
                .findByTenantIdAndJobTypeAndExternalRefId(
                    tenantId, request.getJobType(), request.getExternalRefId());
            if (existing.isPresent()) {
                return toResponse(existing.get());
            }
        }

        // 2. Build job
        Job job = Job.builder()
            .tenantId(tenantId)
            .jobType(request.getJobType())
            .scheduleType(request.getScheduleType())
            .cronExpression(request.getCronExpression())
            .timezone(request.getTimezone())
            .priority(request.getPriority() != null ? request.getPriority() : Priority.NORMAL)
            .misfirePolicy(request.getMisfirePolicy() != null ? request.getMisfirePolicy() : MisfirePolicy.FIRE_NOW)
            .payload(request.getPayload() != null ? request.getPayload() : "{}")
            .maxRetries(request.getMaxRetries() != null ? request.getMaxRetries() : properties.getDefaultMaxRetries())
            .maxRunTimeSec(request.getMaxRunTimeSec() != null ? request.getMaxRunTimeSec() : 300)
            .cancellable(request.getCancellable() != null ? request.getCancellable() : false)
            .externalRefId(request.getExternalRefId())
            .build();

        // 3. Compute next fire time
        if (request.getScheduleType() == ScheduleType.RECURRING) {
            String tz = request.getTimezone() != null ? request.getTimezone() : "UTC";
            cronResolver.validate(request.getCronExpression());
            job.setNextFireTime(cronResolver.nextFireTime(
                request.getCronExpression(), tz, Instant.now()));
            job.setTimezone(tz);
        } else {
            // ONE_OFF or RELATIVE — fire time provided directly
            job.setNextFireTime(Instant.parse(request.getNextFireTime()));
        }

        // 4. Generate ID and assign partition
        job.setId(UUID.randomUUID());
        job.setPartitionId(partitionAssigner.assign(job.getId()));

        // 5. Save
        job = jobRepository.save(job);
        return toResponse(job);
    }

    public JobResponse getJob(UUID tenantId, UUID jobId) {
        Job job = jobRepository.findById(jobId)
            .filter(j -> j.getTenantId().equals(tenantId))
            .orElseThrow(() -> new JobNotFoundException(jobId));
        return toResponse(job);
    }

    public List<JobResponse> listJobs(UUID tenantId) {
        return jobRepository.findByTenantId(tenantId).stream()
            .map(this::toResponse)
            .toList();
    }

    @Transactional
    public JobResponse cancelJob(UUID tenantId, UUID jobId) {
        Job job = jobRepository.findById(jobId)
            .filter(j -> j.getTenantId().equals(tenantId))
            .orElseThrow(() -> new JobNotFoundException(jobId));

        job.setStatus(JobStatus.CANCELLED);
        job.setUpdatedAt(Instant.now());
        jobRepository.save(job);

        // Cancel children too
        jobRepository.findByParentJobId(jobId)
            .forEach(child -> {
                child.setStatus(JobStatus.CANCELLED);
                child.setUpdatedAt(Instant.now());
                jobRepository.save(child);
            });

        return toResponse(job);
    }

    /**
     * Cancel a pending execution (G9: cancel-vs-fire race).
     * Uses status-based atomic check.
     */
    @Transactional
    public boolean cancelPendingExecution(UUID tenantId, UUID jobId) {
        // Cancel the job itself (prevents future fires)
        Job job = jobRepository.findById(jobId)
            .filter(j -> j.getTenantId().equals(tenantId))
            .orElseThrow(() -> new JobNotFoundException(jobId));

        job.setStatus(JobStatus.CANCELLED);
        job.setUpdatedAt(Instant.now());
        jobRepository.save(job);

        // Cancel children too
        jobRepository.findByParentJobId(jobId)
            .forEach(child -> {
                child.setStatus(JobStatus.CANCELLED);
                child.setUpdatedAt(Instant.now());
                jobRepository.save(child);
            });

        // Try to cancel any PENDING execution (the race)
        int cancelled = executionRepository.cancelPendingExecution(jobId);

        if (cancelled > 0) {
            log.info("Cancelled pending execution for job {}", jobId);
            return true;   // cancel won the race
        } else {
            // Either no pending execution, or it's already RUNNING/COMPLETED
            log.info("No pending execution to cancel for job {} (may already be running)", jobId);
            return false;  // fire may have won the race
        }
    }

    /**
     * Manually trigger a job (O2).
     * Creates a new execution with a MANUAL idempotency key.
     * For RECURRING jobs, also advances next_fire_time to prevent
     * double-fire of the upcoming scheduled run.
     */
    @Transactional
    public ExecutionResponse manualTrigger(UUID tenantId, UUID jobId) {
        Job job = jobRepository.findById(jobId)
            .filter(j -> j.getTenantId().equals(tenantId))
            .orElseThrow(() -> new JobNotFoundException(jobId));

        Instant now = Instant.now();
        String idempotencyKey = JobExecution.buildManualKey(jobId, now);

        // Check if a manual trigger with this key already exists
        if (executionRepository.existsByIdempotencyKey(idempotencyKey)) {
            throw new SchedulerException("Manual trigger already in progress for job " + jobId);
        }

        // For recurring jobs: advance next_fire_time to avoid double-fire
        if (job.getScheduleType() == ScheduleType.RECURRING && job.getNextFireTime() != null) {
            // If the next fire is within 2 minutes, skip it
            if (job.getNextFireTime().isBefore(now.plusSeconds(120))) {
                Instant nextAfterSkip = cronResolver.nextFireTime(
                    job.getCronExpression(),
                    job.getTimezone() != null ? job.getTimezone() : "UTC",
                    now
                );
                jobRepository.updateNextFireTime(job.getId(), nextAfterSkip, now);
                log.info("Manual trigger: advanced next fire time for job {} to {}",
                    jobId, nextAfterSkip);
            }
        }

        Long fencingToken = leaseManager.getFencingToken(job.getPartitionId());
        if (fencingToken == null) {
            // This node doesn't own the partition. Set fire time to now so the owner picks it up.
            job.setNextFireTime(now);
            jobRepository.save(job);
            return ExecutionResponse.builder()
                .jobId(jobId)
                .status(ExecutionStatus.PENDING)
                .scheduledFireTime(now)
                .build();
        }

        JobExecution execution = JobExecution.builder()
            .jobId(job.getId())
            .tenantId(job.getTenantId())
            .partitionId(job.getPartitionId())
            .idempotencyKey(idempotencyKey)
            .scheduledFireTime(now)
            .status(ExecutionStatus.PENDING)
            .fencingToken(fencingToken)
            .nodeId(properties.getNodeId())
            .build();
        execution = executionRepository.save(execution);

        return ExecutionResponse.builder()
            .id(execution.getId())
            .jobId(jobId)
            .status(ExecutionStatus.PENDING)
            .scheduledFireTime(now)
            .nodeId(properties.getNodeId())
            .build();
    }

    /**
     * List the next N fire times for a recurring job (API: O3)
     */
    public List<Instant> getNextFireTimes(UUID tenantId, UUID jobId, int count) {
        Job job = jobRepository.findById(jobId)
            .filter(j -> j.getTenantId().equals(tenantId))
            .orElseThrow(() -> new JobNotFoundException(jobId));

        if (job.getScheduleType() != ScheduleType.RECURRING) {
            return job.getNextFireTime() != null
                ? List.of(job.getNextFireTime())
                : List.of();
        }

        return cronResolver.nextFireTimes(
            job.getCronExpression(),
            job.getTimezone() != null ? job.getTimezone() : "UTC",
            Instant.now(), count);
    }

    private JobResponse toResponse(Job job) {
        return JobResponse.builder()
            .id(job.getId())
            .tenantId(job.getTenantId())
            .jobType(job.getJobType())
            .scheduleType(job.getScheduleType())
            .cronExpression(job.getCronExpression())
            .priority(job.getPriority())
            .status(job.getStatus())
            .misfirePolicy(job.getMisfirePolicy())
            .nextFireTime(job.getNextFireTime())
            .partitionId(job.getPartitionId())
            .createdAt(job.getCreatedAt())
            .build();
    }
}

