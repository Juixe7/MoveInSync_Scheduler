package com.movesync.scheduler.api.dto;

import com.movesync.scheduler.domain.model.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class JobResponse {
    private UUID id;
    private UUID tenantId;
    private JobType jobType;
    private ScheduleType scheduleType;
    private String cronExpression;
    private Priority priority;
    private JobStatus status;
    private MisfirePolicy misfirePolicy;
    private Instant nextFireTime;
    private int partitionId;
    private Instant createdAt;
}

