package com.movesync.scheduler.engine;

import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.service.JobService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CancelRaceTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobExecutionRepository executionRepository;

    @InjectMocks
    private JobService jobService;

    @Test
    @DisplayName("G9 Guarantee: Cancel wins the race if execution is still PENDING")
    void cancelWinsWhenExecutionIsPending() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .status(JobStatus.ACTIVE)
            .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(jobRepository.findByParentJobId(jobId)).thenReturn(List.of());

        // Cancel query finds 1 row in PENDING status and atomically cancels it
        when(executionRepository.cancelPendingExecution(eq(jobId))).thenReturn(1);

        boolean cancelled = jobService.cancelPendingExecution(tenantId, jobId);

        assertThat(cancelled).isTrue();
        assertThat(job.getStatus()).isEqualTo(JobStatus.CANCELLED);
        verify(executionRepository).cancelPendingExecution(jobId);
    }

    @Test
    @DisplayName("G9 Guarantee: Fire wins the race if execution was already claimed to RUNNING")
    void fireWinsWhenExecutionIsAlreadyRunning() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .status(JobStatus.ACTIVE)
            .build();

        when(jobRepository.findById(jobId)).thenReturn(Optional.of(job));
        when(jobRepository.findByParentJobId(jobId)).thenReturn(List.of());

        // Execution is already in RUNNING or COMPLETED state, cancel finds 0 PENDING rows
        when(executionRepository.cancelPendingExecution(eq(jobId))).thenReturn(0);

        boolean cancelled = jobService.cancelPendingExecution(tenantId, jobId);

        // Cancel prevented future schedules, but in-flight execution completed normally
        assertThat(cancelled).isFalse();
        assertThat(job.getStatus()).isEqualTo(JobStatus.CANCELLED);
    }
}

