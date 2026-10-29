package com.movesync.scheduler.domain.service;

import java.util.UUID;

public class PartitionAssigner {

    private final int partitionCount;

    public PartitionAssigner(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int assign(UUID jobId) {
        return Math.abs(jobId.hashCode()) % partitionCount;
    }
}

