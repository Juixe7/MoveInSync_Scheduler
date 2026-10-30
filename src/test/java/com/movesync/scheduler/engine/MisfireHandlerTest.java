package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.repository.PartitionLeaseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MisfireHandlerTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobExecutionRepository executionRepository;
    @Mock
    private PartitionLeaseRepository leaseRepository;
    @Mock
    private CronResolver cronResolver;

    private SchedulerProperties properties;
    private MisfireScanner misfireScanner;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        misfireScanner = new MisfireScanner(
            jobRepository,
            executionRepository,
            leaseRepository,
            cronResolver,
            properties
        );
    }

    @Test
    @DisplayName("Misfire Policy FIRE_NOW: Overdue job is left ready for immediate poll pickup")
    void testMisfirePolicyFireNow() {
        UUID jobId = UUID.randomUUID();
        Job job = Job.builder()
            .id(jobId)
            .scheduleType(ScheduleType.ONE_OFF)
            .misfirePolicy(MisfirePolicy.FIRE_NOW)
            .nextFireTime(Instant.now().minus(2, ChronoUnit.HOURS))
            .partitionId(4)
            .build();

        when(executionRepository.findStuckByPartition(4)).thenReturn(List.of());
        when(jobRepository.findOverdueByPartition(eq(4), any())).thenReturn(List.of(job));
        when(executionRepository.existsByIdempotencyKey(any())).thenReturn(false);

        int handled = misfireScanner.scanPartition(4);

        assertThat(handled).isEqualTo(1);
        // FIRE_NOW leaves next_fire_time intact so the next poll cycle runs it
        verify(jobRepository, never()).updateNextFireTime(any(), any(), any());
        verify(jobRepository, never()).updateStatus(any(), any(), any());
    }

    @Test
    @DisplayName("Misfire Policy SKIP: Overdue recurring job skips missed intervals to next future fire time")
    void testMisfirePolicySkipRecurring() {
        UUID jobId = UUID.randomUUID();
        Instant futureFire = Instant.now().plus(1, ChronoUnit.HOURS);

        Job job = Job.builder()
            .id(jobId)
            .scheduleType(ScheduleType.RECURRING)
            .cronExpression("0 0 * * * ?")
            .timezone("UTC")
            .misfirePolicy(MisfirePolicy.SKIP)
            .nextFireTime(Instant.now().minus(3, ChronoUnit.HOURS))
            .partitionId(4)
            .build();

        when(executionRepository.findStuckByPartition(4)).thenReturn(List.of());
        when(jobRepository.findOverdueByPartition(eq(4), any())).thenReturn(List.of(job));
        when(executionRepository.existsByIdempotencyKey(any())).thenReturn(false);
        when(cronResolver.nextFireTime(eq("0 0 * * * ?"), eq("UTC"), any(Instant.class)))
            .thenReturn(futureFire);

        int handled = misfireScanner.scanPartition(4);

        assertThat(handled).isEqualTo(1);
        verify(jobRepository).updateNextFireTime(eq(jobId), eq(futureFire), any(Instant.class));
    }

    @Test
    @DisplayName("Misfire Policy SKIP: Overdue one-off job is marked COMPLETED without firing")
    void testMisfirePolicySkipOneOff() {
        UUID jobId = UUID.randomUUID();
        Job job = Job.builder()
            .id(jobId)
            .scheduleType(ScheduleType.ONE_OFF)
            .misfirePolicy(MisfirePolicy.SKIP)
            .nextFireTime(Instant.now().minus(1, ChronoUnit.HOURS))
            .partitionId(4)
            .build();

        when(executionRepository.findStuckByPartition(4)).thenReturn(List.of());
        when(jobRepository.findOverdueByPartition(eq(4), any())).thenReturn(List.of(job));
        when(executionRepository.existsByIdempotencyKey(any())).thenReturn(false);

        int handled = misfireScanner.scanPartition(4);

        assertThat(handled).isEqualTo(1);
        verify(jobRepository).updateStatus(eq(jobId), eq(JobStatus.COMPLETED), any(Instant.class));
    }

    @Test
    @DisplayName("Misfire Policy COALESCE: Overdue recurring job coalesces missed executions into single fire")
    void testMisfirePolicyCoalesce() {
        UUID jobId = UUID.randomUUID();
        Job job = Job.builder()
            .id(jobId)
            .scheduleType(ScheduleType.RECURRING)
            .cronExpression("0 * * * * ?")
            .timezone("UTC")
            .misfirePolicy(MisfirePolicy.COALESCE)
            .nextFireTime(Instant.now().minus(15, ChronoUnit.MINUTES))
            .partitionId(4)
            .build();

        when(executionRepository.findStuckByPartition(4)).thenReturn(List.of());
        when(jobRepository.findOverdueByPartition(eq(4), any())).thenReturn(List.of(job));
        when(executionRepository.existsByIdempotencyKey(any())).thenReturn(false);
        when(cronResolver.nextFireTime(any(), any(), any())).thenReturn(Instant.now().plus(1, ChronoUnit.MINUTES));

        int handled = misfireScanner.scanPartition(4);

        assertThat(handled).isEqualTo(1);
    }
}

