package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
import com.movesync.scheduler.domain.model.*;
import com.movesync.scheduler.domain.service.CronResolver;
import com.movesync.scheduler.exception.StaleLeaseException;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.repository.JobRepository;
import com.movesync.scheduler.repository.OutboxRepository;
import com.movesync.scheduler.repository.PartitionLeaseRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NodeCrashRecoveryTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobExecutionRepository executionRepository;
    @Mock
    private PartitionLeaseRepository leaseRepository;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private CronResolver cronResolver;

    private SchedulerProperties properties;
    private MisfireScanner misfireScanner;
    private FencedWriter fencedWriter;

    @BeforeEach
    void setUp() {
        properties = new SchedulerProperties();
        properties.setNodeId("node-2");

        misfireScanner = new MisfireScanner(
            jobRepository,
            executionRepository,
            leaseRepository,
            cronResolver,
            properties
        );

        fencedWriter = new FencedWriter(
            executionRepository,
            jobRepository,
            outboxRepository,
            cronResolver
        );
    }

    @Test
    @DisplayName("Crash Recovery: Surviving node recovers in-flight execution when node is killed (kill -9)")
    void shouldRecoverInFlightExecutionWhenNodeIsKilled() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();
        int partitionId = 15;

        // Job that was in-flight on dead node-1
        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .jobType(JobType.PICKUP_ALERT)
            .scheduleType(ScheduleType.ONE_OFF)
            .misfirePolicy(MisfirePolicy.FIRE_NOW)
            .status(JobStatus.ACTIVE)
            .build();

        // Execution created by dead node-1 with old fencing token 10
        JobExecution stuckExec = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .nodeId("node-1")
            .fencingToken(10L)
            .status(ExecutionStatus.RUNNING)
            .idempotencyKey(jobId + ":2026-10-30T10:00:00Z")
            .attemptNumber(1)
            .build();

        // When scanning partition 15, misfireScanner finds the stuck execution
        when(executionRepository.findStuckByPartition(partitionId))
            .thenReturn(List.of(stuckExec));

        // Node-1 has no valid leases (it died / leases expired)
        when(leaseRepository.hasValidLeases("node-1")).thenReturn(false);

        // When finding overdue jobs, no unexecuted jobs remain for this fire time
        when(jobRepository.findOverdueByPartition(eq(partitionId), any())).thenReturn(List.of());

        // Run misfire scan on recovered partition
        int handled = misfireScanner.scanPartition(partitionId);

        // Stuck execution must be marked FAILED with dead node reason for retry
        assertThat(handled).isEqualTo(1);
        verify(executionRepository).updateForRetry(
            eq(execId),
            eq(ExecutionStatus.FAILED),
            eq(1),
            eq("OWNER_NODE_DEAD:node-1")
        );

        // Surviving node-2 takes over partition with bumped fencing token 11
        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.COMPLETED.name()), eq(11L), any(), any()))
            .thenReturn(1);

        // Surviving node completes the job with new fencing token
        stuckExec.setNodeId("node-2");
        stuckExec.setFencingToken(11L);
        stuckExec.setStatus(ExecutionStatus.RUNNING);

        boolean completed = fencedWriter.completeWithFencing(job, stuckExec, "{\"alert\":\"sent\"}");

        assertThat(completed).isTrue();
        verify(jobRepository).updateStatus(eq(jobId), eq(JobStatus.COMPLETED), any());
        verify(outboxRepository).save(any(OutboxEvent.class));
    }

    @Test
    @DisplayName("Crash Recovery: Stale write from killed node is rejected if it attempts commit after takeover")
    void shouldRejectStaleWriteFromKilledNodeAfterTakeover() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();
        int partitionId = 15;

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .jobType(JobType.PICKUP_ALERT)
            .scheduleType(ScheduleType.ONE_OFF)
            .status(JobStatus.ACTIVE)
            .build();

        // Node-1 was frozen or delayed before being killed, holding stale token 10
        JobExecution staleExec = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .nodeId("node-1")
            .fencingToken(10L)
            .status(ExecutionStatus.RUNNING)
            .build();

        // In DB, partition lease was taken over by node-2 with token 11.
        // Fenced write query returns 0 affected rows because pl.fencing_token = 11 != 10
        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.COMPLETED.name()), eq(10L), any(), any()))
            .thenReturn(0);

        assertThatThrownBy(() -> fencedWriter.completeWithFencing(job, staleExec, "{\"alert\":\"stale\"}"))
            .isInstanceOf(StaleLeaseException.class)
            .hasMessageContaining("Fencing rejected");

        // Outbox event and job completion must NEVER be saved by stale write
        verify(outboxRepository, never()).save(any());
        verify(jobRepository, never()).updateStatus(any(), any(), any());
    }

    @Test
    @DisplayName("Crash Recovery: 1,200 pickup alerts simulation across recovered partitions")
    void shouldRecoverAll1200PickupAlertsAcrossPartitions() {
        int totalAlerts = 1200;
        UUID tenantId = UUID.randomUUID();
        List<Job> allAlerts = new ArrayList<>();
        Map<Integer, List<Job>> alertsByPartition = new HashMap<>();

        for (int i = 0; i < totalAlerts; i++) {
            int partition = i % 64;
            Job job = Job.builder()
                .id(UUID.randomUUID())
                .tenantId(tenantId)
                .partitionId(partition)
                .jobType(JobType.PICKUP_ALERT)
                .scheduleType(ScheduleType.ONE_OFF)
                .misfirePolicy(MisfirePolicy.FIRE_NOW)
                .nextFireTime(Instant.now().minusSeconds(10))
                .status(JobStatus.ACTIVE)
                .build();
            allAlerts.add(job);
            alertsByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(job);
        }

        // Suppose Node-1 crashed while owning partitions 0..21 (which held ~412 jobs)
        List<Integer> node1Partitions = new ArrayList<>();
        for (int p = 0; p <= 21; p++) {
            node1Partitions.add(p);
        }

        // Surviving nodes claim orphaned partitions 0..21
        int recoveredCount = 0;
        for (int p : node1Partitions) {
            List<Job> partitionJobs = alertsByPartition.getOrDefault(p, Collections.emptyList());
            when(jobRepository.findOverdueByPartition(eq(p), any())).thenReturn(partitionJobs);
            when(executionRepository.findStuckByPartition(p)).thenReturn(Collections.emptyList());

            recoveredCount += misfireScanner.scanPartition(p);
        }

        int expectedInPartitions = node1Partitions.stream()
            .mapToInt(p -> alertsByPartition.getOrDefault(p, Collections.emptyList()).size())
            .sum();

        // Every single overdue alert in the dead node's partitions was caught by MisfireScanner
        assertThat(recoveredCount).isEqualTo(expectedInPartitions);
    }
}
