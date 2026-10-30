package com.movesync.scheduler.engine;

import com.movesync.scheduler.config.SchedulerProperties;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NodeFreezeTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobExecutionRepository executionRepository;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private CronResolver cronResolver;

    private FencedWriter fencedWriter;

    @BeforeEach
    void setUp() {
        fencedWriter = new FencedWriter(
            executionRepository,
            jobRepository,
            outboxRepository,
            cronResolver
        );
    }

    @Test
    @DisplayName("G4 Guarantee: Frozen node waking up after 30s has stale write rejected via fencing token")
    void shouldRejectStaleWriteWhenFrozenNodeResumes() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execIdNode1 = UUID.randomUUID();
        UUID execIdNode2 = UUID.randomUUID();
        int partitionId = 9;

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .jobType(JobType.PICKUP_ALERT)
            .scheduleType(ScheduleType.ONE_OFF)
            .status(JobStatus.ACTIVE)
            .build();

        // 1. Node 1 holds stale token 4 from before the 30s GC pause
        JobExecution frozenNodeExecution = JobExecution.builder()
            .id(execIdNode1)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .nodeId("node-3")
            .fencingToken(4L)
            .status(ExecutionStatus.RUNNING)
            .build();

        // 2. While Node 1 was frozen, Node 2 claimed partition 9 with incremented token 5
        JobExecution newOwnerExecution = JobExecution.builder()
            .id(execIdNode2)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .nodeId("node-1")
            .fencingToken(5L)
            .status(ExecutionStatus.RUNNING)
            .build();

        // Node 2's commit succeeds because fencing token 5 matches partition_leases in DB
        when(executionRepository.fencedComplete(eq(execIdNode2), eq(ExecutionStatus.COMPLETED.name()), eq(5L), any(), any()))
            .thenReturn(1);

        boolean node2Committed = fencedWriter.completeWithFencing(job, newOwnerExecution, "{\"pickup\":\"sent\"}");
        assertThat(node2Committed).isTrue();
        verify(outboxRepository, times(1)).save(any(OutboxEvent.class));
        verify(jobRepository, times(1)).updateStatus(eq(jobId), eq(JobStatus.COMPLETED), any(Instant.class));

        // 3. Frozen Node 1 now unpauses (SIGCONT) and attempts to commit with stale token 4.
        // DB query returns 0 rows updated because pl.fencing_token = 5 != 4
        when(executionRepository.fencedComplete(eq(execIdNode1), eq(ExecutionStatus.COMPLETED.name()), eq(4L), any(), any()))
            .thenReturn(0);

        assertThatThrownBy(() -> fencedWriter.completeWithFencing(job, frozenNodeExecution, "{\"pickup\":\"stale\"}"))
            .isInstanceOf(StaleLeaseException.class)
            .hasMessageContaining("Fencing rejected: partition=9 staleToken=4");

        // Assert: No second outbox event is saved, no second status update occurs
        verify(outboxRepository, times(1)).save(any(OutboxEvent.class));
        verify(jobRepository, times(1)).updateStatus(eq(jobId), eq(JobStatus.COMPLETED), any(Instant.class));
    }

    @Test
    @DisplayName("G4 Guarantee: Dead-letter write from frozen node is also rejected via fencing check")
    void shouldRejectStaleDeadLetterFromFrozenNode() {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        UUID execId = UUID.randomUUID();
        int partitionId = 9;

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .jobType(JobType.NO_SHOW_CANCEL)
            .scheduleType(ScheduleType.ONE_OFF)
            .status(JobStatus.ACTIVE)
            .build();

        JobExecution staleExec = JobExecution.builder()
            .id(execId)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .nodeId("node-3")
            .fencingToken(4L)
            .status(ExecutionStatus.RUNNING)
            .build();

        when(executionRepository.fencedComplete(eq(execId), eq(ExecutionStatus.DEAD_LETTERED.name()), eq(4L), any(), any()))
            .thenReturn(0);

        boolean deadLetterResult = fencedWriter.deadLetterWithFencing(job, staleExec, "Timeout error");

        assertThat(deadLetterResult).isFalse();
        verify(outboxRepository, never()).save(any());
        verify(jobRepository, never()).updateStatus(any(), any(), any());
    }

    @Test
    @DisplayName("G4 Guarantee: Concurrent completion race between stale and valid tokens")
    void shouldHandleConcurrentCompletionRaceAtomically() throws Exception {
        UUID jobId = UUID.randomUUID();
        UUID tenantId = UUID.randomUUID();
        int partitionId = 9;

        Job job = Job.builder()
            .id(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .jobType(JobType.ROSTER_GENERATION)
            .scheduleType(ScheduleType.ONE_OFF)
            .status(JobStatus.ACTIVE)
            .build();

        UUID exec1 = UUID.randomUUID();
        UUID exec2 = UUID.randomUUID();

        JobExecution staleExec = JobExecution.builder()
            .id(exec1)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .fencingToken(7L)
            .status(ExecutionStatus.RUNNING)
            .build();

        JobExecution validExec = JobExecution.builder()
            .id(exec2)
            .jobId(jobId)
            .tenantId(tenantId)
            .partitionId(partitionId)
            .fencingToken(8L)
            .status(ExecutionStatus.RUNNING)
            .build();

        when(executionRepository.fencedComplete(eq(exec1), anyString(), eq(7L), any(), any())).thenReturn(0);
        when(executionRepository.fencedComplete(eq(exec2), anyString(), eq(8L), any(), any())).thenReturn(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);

        Future<?> f1 = pool.submit(() -> {
            try {
                barrier.await();
                fencedWriter.completeWithFencing(job, staleExec, "{}");
                successCount.incrementAndGet();
            } catch (StaleLeaseException e) {
                rejectedCount.incrementAndGet();
            } catch (Exception ignored) {}
        });

        Future<?> f2 = pool.submit(() -> {
            try {
                barrier.await();
                fencedWriter.completeWithFencing(job, validExec, "{}");
                successCount.incrementAndGet();
            } catch (StaleLeaseException e) {
                rejectedCount.incrementAndGet();
            } catch (Exception ignored) {}
        });

        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);
        pool.shutdown();

        // Exactly one commit must succeed and exactly one must be rejected
        assertThat(successCount.get()).isEqualTo(1);
        assertThat(rejectedCount.get()).isEqualTo(1);
    }
}
