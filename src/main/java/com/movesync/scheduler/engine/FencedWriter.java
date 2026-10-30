package com.movesync.scheduler.engine;

import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.exception.StaleLeaseException;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
@Slf4j
@RequiredArgsConstructor
public class FencedWriter {

    private final JobExecutionRepository executionRepository;
    private final JobRepository jobRepository;
    private final OutboxRepository outboxRepository;
    private final CronResolver cronResolver;

    /**
     * Complete a job execution with fencing check.
     * ENTIRE method runs in ONE transaction.
     * If fencing fails → StaleLeaseException → transaction rolls back → all writes undone.
     *
     * @return true if committed, false if fencing rejected
     */
    @Transactional
    public boolean completeWithFencing(Job job, JobExecution execution, String result) {
        // G4: fenced write
        int affected = executionRepository.fencedComplete(
            execution.getId(),
            ExecutionStatus.COMPLETED.name(),
            execution.getFencingToken(),
            result,
            null
        );

        if (affected == 0) {
            log.warn("FENCING_REJECTED: job={} partition={} token={}",
                job.getId(), job.getPartitionId(), execution.getFencingToken());
            throw new StaleLeaseException(job.getPartitionId(), execution.getFencingToken());
            // Transaction rolls back — no side effects persist
        }

        // Outbox event (same transaction)
        outboxRepository.save(OutboxEvent.builder()
            .aggregateType("JOB")
            .aggregateId(job.getId())
            .eventType("JOB_COMPLETED")
            .payload(String.format(
                "{\"jobId\":\"%s\",\"tenantId\":\"%s\",\"fireLagMs\":%d}",
                job.getId(), job.getTenantId(),
                execution.getFireLagMs() != null ? execution.getFireLagMs() : 0))
            .build());

        // Advance next fire time or mark complete
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
            job.getId(), job.getJobType(), execution.getFireLagMs());

        return true;
    }

    /**
     * Dead-letter a job execution with fencing check.
     */
    @Transactional
    public boolean deadLetterWithFencing(Job job, JobExecution execution, String error) {
        int affected = executionRepository.fencedComplete(
            execution.getId(),
            ExecutionStatus.DEAD_LETTERED.name(),
            execution.getFencingToken(),
            null,
            error
        );

        if (affected == 0) {
            log.warn("FENCING_REJECTED on dead-letter: job={} partition={} token={}",
                job.getId(), job.getPartitionId(), execution.getFencingToken());
            return false;
        }

        outboxRepository.save(OutboxEvent.builder()
            .aggregateType("JOB")
            .aggregateId(job.getId())
            .eventType("JOB_DEAD_LETTERED")
            .payload(String.format(
                "{\"jobId\":\"%s\",\"tenantId\":\"%s\",\"error\":\"%s\"}",
                job.getId(), job.getTenantId(), error))
            .build());

        log.error("Job dead-lettered: id={} attempts={} error={}",
            job.getId(), execution.getAttemptNumber(), error);

        return true;
    }
}

