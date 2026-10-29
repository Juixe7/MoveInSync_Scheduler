package com.movesync.scheduler.exception;

public class TenantAccessDeniedException extends SchedulerException {
    public TenantAccessDeniedException(String message) {
        super(message);
    }
}

