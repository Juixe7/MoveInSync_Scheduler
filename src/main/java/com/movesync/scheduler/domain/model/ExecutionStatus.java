package com.movesync.scheduler.domain.model;

public enum ExecutionStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    RETRYING,
    DEAD_LETTERED,
    CANCELLED_BEFORE_EXECUTION,
    SKIPPED;

    public boolean isTerminal() {
        return this == COMPLETED || this == DEAD_LETTERED
            || this == CANCELLED_BEFORE_EXECUTION || this == SKIPPED;
    }
}

