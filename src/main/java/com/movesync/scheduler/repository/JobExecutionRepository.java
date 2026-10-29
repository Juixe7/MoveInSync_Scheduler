package com.movesync.scheduler.repository;

import com.movesync.scheduler.domain.model.JobExecution;
import com.movesync.scheduler.domain.model.ExecutionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobExecutionRepository extends JpaRepository<JobExecution, UUID> {

    // Idempotency check
    Optional<JobExecution> findByIdempotencyKey(String idempotencyKey);

    boolean existsByIdempotencyKey(String idempotencyKey);

    // Job execution history
    List<JobExecution> findByJobIdOrderByCreatedAtDesc(UUID jobId);

    // Dead-letter queries for operators
    List<JobExecution> findByTenantIdAndStatus(UUID tenantId, ExecutionStatus status);

    // The G4 fenced completion write — CRITICAL
    // Returns affected row count: 1 = success, 0 = fencing rejected
    @Modifying
    @Query(value = """
        UPDATE job_executions je
        SET status = :status,
            completed_at = NOW(),
            result = CAST(:result AS jsonb),
            error_message = :errorMessage
        FROM partition_leases pl
        WHERE je.id = :execId
          AND je.partition_id = pl.partition_id
          AND pl.fencing_token = :fencingToken
        """, nativeQuery = true)
    int fencedComplete(
        @Param("execId") UUID execId,
        @Param("status") String status,
        @Param("fencingToken") long fencingToken,
        @Param("result") String result,
        @Param("errorMessage") String errorMessage
    );

    // Update status for retry
    @Modifying
    @Query("""
        UPDATE JobExecution je 
        SET je.status = :status, 
            je.attemptNumber = :attempt,
            je.errorMessage = :error
        WHERE je.id = :execId
        """)
    void updateForRetry(
        @Param("execId") UUID execId,
        @Param("status") ExecutionStatus status,
        @Param("attempt") int attempt,
        @Param("error") String error
    );

    // Find stuck RUNNING executions (for misfire scanner)
    @Query(value = """
        SELECT * FROM job_executions
        WHERE status = 'RUNNING'
          AND node_id = :nodeId
          AND created_at < NOW() - INTERVAL '1 minute' * :maxRunTimeMin
        """, nativeQuery = true)
    List<JobExecution> findStuckExecutions(
        @Param("nodeId") String nodeId,
        @Param("maxRunTimeMin") int maxRunTimeMin
    );
}

