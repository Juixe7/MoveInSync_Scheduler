package com.movesync.scheduler.service;

import com.movesync.scheduler.api.dto.*;
import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.domain.service.PartitionAssigner;
import com.movesync.scheduler.exception.JobNotFoundException;
import com.movesync.scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobRepository;
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

