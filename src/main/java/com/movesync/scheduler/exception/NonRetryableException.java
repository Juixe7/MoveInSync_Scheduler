package com.movesync.scheduler.exception;

public class NonRetryableException extends SchedulerException {
    public NonRetryableException(String message) {
        super(message);
    }
    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}

