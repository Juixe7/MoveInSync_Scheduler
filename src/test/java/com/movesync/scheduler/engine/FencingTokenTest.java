package com.movesync.scheduler.engine;

import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.exception.StaleLeaseException;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FencingTokenTest {

    @Mock
    private JobExecutionRepository executionRepository;
    @Mock
    private JobRepository jobRepository;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private CronResolver cronResolver;

    private FencedWriter fencedWriter;

    @BeforeEach
    void setUp() {
        fencedWriter = new FencedWriter(executionRepository, jobRepository, outboxRepository, cronResolver);
    }

    @Test
    @DisplayName("G4 Guarantee: Successful write when fencing token matches active partition lease")
    void shouldCompleteExecutionWhenFencingTokenMatches() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .jobType(JobType.PICKUP_ALERT)
            .scheduleType(ScheduleType.ONE_OFF)
            .partitionId(5)
            .build();

        JobExecution execution = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(5)
            .fencingToken(3L)
            .status(ExecutionStatus.RUNNING)
            .build();

        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.COMPLETED.name()), eq(3L), any(), any()))
            .thenReturn(1);

        boolean success = fencedWriter.completeWithFencing(job, execution, "{\"status\":\"ok\"}");

        assertThat(success).isTrue();
        verify(outboxRepository).save(any(OutboxEvent.class));
        verify(jobRepository).updateStatus(eq(jobId), eq(JobStatus.COMPLETED), any(Instant.class));
    }

    @Test
    @DisplayName("G4 Guarantee: Stale write is rejected with StaleLeaseException when fencing token is stale")
    void shouldThrowStaleLeaseExceptionWhenFencingTokenRejected() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .jobType(JobType.BILLING_CLOSE)
            .scheduleType(ScheduleType.ONE_OFF)
            .partitionId(5)
            .build();

        // Stale execution has token 2, while current partition lease in DB was bumped to 3 by another node
        JobExecution execution = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(5)
            .fencingToken(2L)
            .status(ExecutionStatus.RUNNING)
            .build();

        // fencedComplete returns 0 because token does not match active lease in DB
        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.COMPLETED.name()), eq(2L), any(), any()))
            .thenReturn(0);

        assertThatThrownBy(() -> fencedWriter.completeWithFencing(job, execution, null))
            .isInstanceOf(StaleLeaseException.class)
            .hasMessageContaining("Fencing rejected: partition=5 staleToken=2");

        // Outbox event and job status update MUST NOT be written on rejected stale write
        verifyNoInteractions(outboxRepository);
        verify(jobRepository, never()).updateStatus(any(), any(), any());
        verify(jobRepository, never()).updateNextFireTime(any(), any(), any());
    }

    @Test
    @DisplayName("G4 Guarantee: Dead-letter write is rejected when fencing token is stale")
    void shouldRejectDeadLetterWhenFencingTokenStale() {
        UUID jobId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();

        Job job = Job.builder()
            .id(jobId)
            .partitionId(12)
            .build();

        JobExecution execution = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .partitionId(12)
            .fencingToken(1L)
            .build();

        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.DEAD_LETTERED.name()), eq(1L), any(), eq("Timeout")))
            .thenReturn(0);

        boolean result = fencedWriter.deadLetterWithFencing(job, execution, "Timeout");

        assertThat(result).isFalse();
        verifyNoInteractions(outboxRepository);
    }
}

