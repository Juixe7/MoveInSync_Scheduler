package com.movesync.scheduler.domain.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_executions")
@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class JobExecution {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "partition_id", nullable = false)
    private int partitionId;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "scheduled_fire_time", nullable = false)
    private Instant scheduledFireTime;

    @Column(name = "actual_start_time")
    private Instant actualStartTime;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private ExecutionStatus status = ExecutionStatus.PENDING;

    @Column(name = "attempt_number", nullable = false)
    @Builder.Default
    private int attemptNumber = 1;

    @Column(name = "fencing_token", nullable = false)
    private long fencingToken;

    @Column(name = "node_id", nullable = false)
    private String nodeId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String result;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "fire_lag_ms")
    private Long fireLagMs;

    @Column(nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    /**
     * Build the idempotency key for a scheduled run.
     */
    public static String buildIdempotencyKey(UUID jobId, Instant fireTime) {
        return jobId.toString() + ":" + fireTime.toString();
    }

    /**
     * Build the idempotency key for a manual trigger.
     */
    public static String buildManualKey(UUID jobId, Instant triggerTime) {
        return jobId.toString() + ":MANUAL:" + triggerTime.toString();
    }

    /**
     * Compute fire lag from scheduled vs actual start.
     */
    public void computeFireLag() {
        if (actualStartTime != null && scheduledFireTime != null) {
            this.fireLagMs = actualStartTime.toEpochMilli() - scheduledFireTime.toEpochMilli();
        }
    }
}

