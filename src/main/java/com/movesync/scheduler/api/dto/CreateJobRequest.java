package com.movesync.scheduler.api.dto;

import com.movesync.scheduler.domain.model.*;
import jakarta.validation.constraints.*;
import lombok.*;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class CreateJobRequest {

    @NotNull
    private JobType jobType;

    @NotNull
    private ScheduleType scheduleType;

    private String cronExpression;       // required if RECURRING
    private String timezone;             // optional override
    private String nextFireTime;         // ISO-8601, required if ONE_OFF or RELATIVE

    private Priority priority;           // defaults to NORMAL
    private MisfirePolicy misfirePolicy; // defaults to FIRE_NOW
    private String externalRefId;        // for idempotent creation
    private String payload;              // JSON string

    private Integer maxRetries;
    private Integer maxRunTimeSec;
    private Boolean cancellable;
}

