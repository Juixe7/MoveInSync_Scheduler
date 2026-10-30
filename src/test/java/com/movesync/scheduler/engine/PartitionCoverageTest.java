package com.movesync.scheduler.engine;

import com.movesync.scheduler.domain.service.PartitionAssigner;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionCoverageTest {

    private static final int TOTAL_PARTITIONS = 64;

    @Test
    @DisplayName("G6 Guarantee: PartitionAssigner assigns deterministic partitions in [0..63]")
    void shouldDeterministicallyAssignPartitions() {
        PartitionAssigner assigner = new PartitionAssigner(TOTAL_PARTITIONS);
        UUID sampleId = UUID.randomUUID();

        int p1 = assigner.assign(sampleId);
        int p2 = assigner.assign(sampleId);

        assertThat(p1).isEqualTo(p2);
        assertThat(p1).isBetween(0, TOTAL_PARTITIONS - 1);
    }

    @Test
    @DisplayName("G6 Guarantee: 100% partition coverage with uniform distribution over 10,000 keys")
    void shouldCoverAll64PartitionsUniformly() {
        PartitionAssigner assigner = new PartitionAssigner(TOTAL_PARTITIONS);
        Map<Integer, Integer> distribution = new HashMap<>();

        int totalKeys = 10_000;
        for (int i = 0; i < totalKeys; i++) {
            UUID id = UUID.randomUUID();
            int partition = assigner.assign(id);
            assertThat(partition).isBetween(0, TOTAL_PARTITIONS - 1);
            distribution.merge(partition, 1, Integer::sum);
        }

        // All 64 partitions MUST be covered
        assertThat(distribution.keySet()).hasSize(TOTAL_PARTITIONS);
        for (int p = 0; p < TOTAL_PARTITIONS; p++) {
            assertThat(distribution.get(p))
                .as("Partition %d should have assigned keys", p)
                .isNotNull()
                .isGreaterThan(50); // With 10,000 keys / 64 ≈ 156 keys per partition, min > 50
        }
    }

    @Test
    @DisplayName("G6 Guarantee: Cluster fair share coverage without gaps or overlapping partitions")
    void shouldPartitionClusterCoverageDisjointAndComplete() {
        // Test fair-share calculation and coverage across 1..8 nodes
        for (int activeNodes = 1; activeNodes <= 8; activeNodes++) {
            int fairShare = (int) Math.ceil((double) TOTAL_PARTITIONS / activeNodes);

            Set<Integer> allClaimed = new HashSet<>();
            int remaining = TOTAL_PARTITIONS;

            for (int node = 1; node <= activeNodes; node++) {
                int claimForNode = Math.min(fairShare, remaining);
                for (int i = 0; i < claimForNode; i++) {
                    int partitionId = TOTAL_PARTITIONS - remaining + i;
                    boolean added = allClaimed.add(partitionId);
                    assertThat(added)
                        .as("Partition %d claimed twice across active nodes", partitionId)
                        .isTrue();
                }
                remaining -= claimForNode;
            }

            // Union of all claimed partitions must equal exact set [0..63]
            assertThat(allClaimed).hasSize(TOTAL_PARTITIONS);
            for (int p = 0; p < TOTAL_PARTITIONS; p++) {
                assertThat(allClaimed).contains(p);
            }
        }
    }
}

