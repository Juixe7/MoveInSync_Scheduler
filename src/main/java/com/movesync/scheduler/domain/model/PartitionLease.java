package com.movesync.scheduler.domain.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(name = "partition_leases")
@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class PartitionLease {

    @Id
    @Column(name = "partition_id")
    private int partitionId;

    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "fencing_token", nullable = false)
    @Builder.Default
    private long fencingToken = 0;

    @Column(name = "lease_expires")
    private Instant leaseExpires;

    @Column(name = "acquired_at")
    private Instant acquiredAt;

    @Column(name = "updated_at", nullable = false)
    @Builder.Default
    private Instant updatedAt = Instant.now();

    public boolean isExpired() {
        return leaseExpires == null || Instant.now().isAfter(leaseExpires);
    }

    public boolean isOwnedBy(String nodeId) {
        return this.nodeId != null && this.nodeId.equals(nodeId) && !isExpired();
    }
}

