package com.movesync.scheduler.api.dto;

import com.movesync.scheduler.domain.model.ExecutionStatus;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class ExecutionResponse {
    private UUID id;
    private UUID jobId;
    private ExecutionStatus status;
    private int attemptNumber;
    private Instant scheduledFireTime;
    private Instant actualStartTime;
    private Instant completedAt;
    private Long fireLagMs;
    private String nodeId;
    private String errorMessage;  // never contains PII
}

