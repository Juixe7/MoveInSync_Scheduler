package com.movesync.scheduler.handler;

import com.movesync.scheduler.domain.model.JobExecution;

public interface JobHandler {
    void handle(JobExecution execution) throws Exception;
}

