package com.movesync.scheduler.exception;

import java.util.UUID;

public class JobNotFoundException extends SchedulerException {
    public JobNotFoundException(UUID jobId) {
        super("Job not found: " + jobId);
    }
}

