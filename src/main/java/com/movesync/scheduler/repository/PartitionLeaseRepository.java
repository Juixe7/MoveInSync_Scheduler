package com.movesync.scheduler.repository;

import com.movesync.scheduler.domain.model.PartitionLease;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface PartitionLeaseRepository extends JpaRepository<PartitionLease, Integer> {

    // Find all partitions owned by this node
    @Query(value = """
        SELECT * FROM partition_leases
        WHERE node_id = :nodeId AND lease_expires > NOW()
        """, nativeQuery = true)
    List<PartitionLease> findByActiveNode(@Param("nodeId") String nodeId);

    // Claim unclaimed/expired partitions — with SKIP LOCKED
    // Returns number of rows claimed
    @Modifying
    @Query(value = """
        UPDATE partition_leases
        SET node_id = :nodeId,
            fencing_token = fencing_token + 1,
            lease_expires = NOW() + CAST(:timeoutSec || ' seconds' AS INTERVAL),
            acquired_at = NOW(),
            updated_at = NOW()
        WHERE partition_id IN (
            SELECT partition_id
            FROM partition_leases
            WHERE node_id IS NULL OR lease_expires < NOW()
            FOR UPDATE SKIP LOCKED
            LIMIT :maxClaim
        )
        """, nativeQuery = true)
    int claimPartitions(
        @Param("nodeId") String nodeId,
        @Param("maxClaim") int maxClaim,
        @Param("timeoutSec") int timeoutSec
    );

    // Renew leases — uses DB clock, not JVM clock
    @Modifying
    @Query(value = """
        UPDATE partition_leases
        SET lease_expires = NOW() + CAST(:timeoutSec || ' seconds' AS INTERVAL),
            updated_at = NOW()
        WHERE node_id = :nodeId
          AND lease_expires > NOW()
        """, nativeQuery = true)
    int renewLeases(
        @Param("nodeId") String nodeId,
        @Param("timeoutSec") int timeoutSec
    );

    // Release partitions (graceful shutdown or voluntary shedding)
    @Modifying
    @Query(value = """
        UPDATE partition_leases
        SET node_id = NULL,
            lease_expires = NOW() - INTERVAL '1 second',
            updated_at = NOW()
        WHERE node_id = :nodeId
          AND partition_id IN (:partitionIds)
        """, nativeQuery = true)
    void releasePartitions(
        @Param("nodeId") String nodeId,
        @Param("partitionIds") List<Integer> partitionIds
    );

    // Release ALL partitions for this node (shutdown)
    @Modifying
    @Query(value = """
        UPDATE partition_leases
        SET node_id = NULL,
            lease_expires = NOW() - INTERVAL '1 second',
            updated_at = NOW()
        WHERE node_id = :nodeId
        """, nativeQuery = true)
    void releaseAllPartitions(@Param("nodeId") String nodeId);

    // Heartbeat node presence
    @Modifying
    @Query(value = """
        INSERT INTO node_heartbeats (node_id, last_heartbeat)
        VALUES (:nodeId, NOW())
        ON CONFLICT (node_id) DO UPDATE
        SET last_heartbeat = NOW()
        """, nativeQuery = true)
    void heartbeatNode(@Param("nodeId") String nodeId);

    // Remove node on graceful shutdown
    @Modifying
    @Query(value = "DELETE FROM node_heartbeats WHERE node_id = :nodeId", nativeQuery = true)
    void removeNode(@Param("nodeId") String nodeId);

    // Count distinct active nodes (for fair-share calculation)
    @Query(value = """
        SELECT GREATEST(
            (SELECT COUNT(*) FROM node_heartbeats WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'),
            (SELECT COUNT(DISTINCT node_id) FROM partition_leases WHERE node_id IS NOT NULL AND lease_expires > NOW())
        )
        """, nativeQuery = true)
    int countActiveNodes();

    // Check if a specific node has any valid leases
    @Query(value = """
        SELECT COUNT(*) > 0
        FROM partition_leases
        WHERE node_id = :nodeId AND lease_expires > NOW()
        """, nativeQuery = true)
    boolean hasValidLeases(@Param("nodeId") String nodeId);

    // Get fencing token for a partition (for the executor to read)
    @Query(value = """
        SELECT fencing_token FROM partition_leases
        WHERE partition_id = :partitionId AND node_id = :nodeId
        """, nativeQuery = true)
    Long getFencingToken(
        @Param("partitionId") int partitionId,
        @Param("nodeId") String nodeId
    );
}

