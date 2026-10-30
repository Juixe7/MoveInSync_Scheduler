package com.movesync.scheduler.repository;

import com.movesync.scheduler.domain.model.Job;
import com.movesync.scheduler.domain.model.JobStatus;
import com.movesync.scheduler.domain.model.JobType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    // The critical poll query — FOR UPDATE SKIP LOCKED
    @Query(value = """
        SELECT * FROM jobs
        WHERE partition_id IN (:partitionIds)
          AND status = 'ACTIVE'
          AND next_fire_time <= NOW()
        ORDER BY
          CASE priority
            WHEN 'CRITICAL' THEN 0
            WHEN 'HIGH' THEN 1
            WHEN 'NORMAL' THEN 2
            WHEN 'LOW' THEN 3
          END ASC,
          next_fire_time ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Job> findDueJobs(
        @Param("partitionIds") List<Integer> partitionIds,
        @Param("limit") int limit
    );

    // Idempotent creation check
    Optional<Job> findByTenantIdAndJobTypeAndExternalRefId(
        UUID tenantId, JobType jobType, String externalRefId
    );

    // Tenant-scoped queries
    List<Job> findByTenantIdAndStatus(UUID tenantId, JobStatus status);

    List<Job> findByTenantId(UUID tenantId);

    // For cascading cancel (parent → children)
    List<Job> findByParentJobId(UUID parentJobId);

    // For manual trigger: advance next fire time
    @Modifying
    @Query("UPDATE Job j SET j.nextFireTime = :nextFireTime, j.updatedAt = :now WHERE j.id = :jobId")
    void updateNextFireTime(
        @Param("jobId") UUID jobId,
        @Param("nextFireTime") Instant nextFireTime,
        @Param("now") Instant now
    );

    // Mark job complete (ONE_OFF jobs after execution)
    @Modifying
    @Query("UPDATE Job j SET j.status = :status, j.updatedAt = :now WHERE j.id = :jobId")
    void updateStatus(
        @Param("jobId") UUID jobId,
        @Param("status") JobStatus status,
        @Param("now") Instant now
    );

    // Find overdue jobs in a specific partition
    @Query(value = """
        SELECT * FROM jobs
        WHERE partition_id = :partitionId
          AND status = 'ACTIVE'
          AND next_fire_time IS NOT NULL
          AND next_fire_time < NOW()
          AND next_fire_time > :lookbackLimit
        ORDER BY next_fire_time ASC
        """, nativeQuery = true)
    List<Job> findOverdueByPartition(
        @Param("partitionId") int partitionId,
        @Param("lookbackLimit") Instant lookbackLimit
    );
}

