package com.movesync.scheduler.handler;

import com.movesync.scheduler.domain.model.JobType;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

@Component
public class JobHandlerRegistry {

    private final Map<JobType, JobHandler> handlers = new EnumMap<>(JobType.class);

    public void register(JobType type, JobHandler handler) {
        handlers.put(type, handler);
    }

    public JobHandler getHandler(JobType type) {
        JobHandler handler = handlers.get(type);
        if (handler == null) {
            throw new IllegalArgumentException("No handler for job type: " + type);
        }
        return handler;
    }
}

