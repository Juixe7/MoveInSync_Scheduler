package com.movesync.scheduler.exception;

public class StaleLeaseException extends SchedulerException {
    public StaleLeaseException(int partitionId, long staleToken) {
        super("Fencing rejected: partition=" + partitionId + " staleToken=" + staleToken);
    }
}

