package com.movesync.scheduler.domain.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs")
@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class Job implements Persistable<UUID> {

    @Id
    private UUID id;

    @Transient
    @Builder.Default
    private boolean isNew = true;

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    void markNotNew() {
        this.isNew = false;
    }

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "external_ref_id")
    private String externalRefId;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type", nullable = false)
    private JobType jobType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private Priority priority = Priority.NORMAL;

    @Enumerated(EnumType.STRING)
    @Column(name = "schedule_type", nullable = false)
    private ScheduleType scheduleType;

    @Column(name = "cron_expression")
    private String cronExpression;

    private String timezone;

    @Column(name = "next_fire_time")
    private Instant nextFireTime;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    @Builder.Default
    private String payload = "{}";

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private JobStatus status = JobStatus.ACTIVE;

    @Enumerated(EnumType.STRING)
    @Column(name = "misfire_policy", nullable = false)
    @Builder.Default
    private MisfirePolicy misfirePolicy = MisfirePolicy.FIRE_NOW;

    @Column(name = "max_retries", nullable = false)
    @Builder.Default
    private int maxRetries = 3;

    @Column(name = "max_run_time_sec", nullable = false)
    @Builder.Default
    private int maxRunTimeSec = 300;

    @Column(nullable = false)
    @Builder.Default
    private boolean cancellable = false;

    @Column(name = "parent_job_id")
    private UUID parentJobId;

    @Column(name = "partition_id", nullable = false)
    private int partitionId;

    @Column(nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @Column(nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    /**
     * Compute partition from job ID. Call after ID is generated.
     */
    public void assignPartition(int partitionCount) {
        this.partitionId = Math.abs(this.id.hashCode()) % partitionCount;
    }
}

