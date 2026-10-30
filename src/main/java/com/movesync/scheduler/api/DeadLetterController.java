package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.ExecutionResponse;
import com.movesync.scheduler.domain.model.ExecutionStatus;
import com.movesync.scheduler.domain.model.JobExecution;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants/{tenantId}/dead-letter")
@RequiredArgsConstructor
public class DeadLetterController {

    private final JobExecutionRepository executionRepository;
    private final JobService jobService;

    @GetMapping
    public ResponseEntity<List<ExecutionResponse>> listDeadLettered(
            @PathVariable UUID tenantId) {
        List<ExecutionResponse> deadLettered = executionRepository
            .findByTenantIdAndStatus(tenantId, ExecutionStatus.DEAD_LETTERED)
            .stream()
            .map(this::toResponse)
            .toList();
        return ResponseEntity.ok(deadLettered);
    }

    /**
     * Replay a dead-lettered execution (O4).
     * Triggers the original job manually.
     */
    @PostMapping("/{executionId}/replay")
    public ResponseEntity<?> replay(
            @PathVariable UUID tenantId,
            @PathVariable UUID executionId) {
        JobExecution deadExec = executionRepository.findById(executionId)
            .filter(e -> e.getTenantId().equals(tenantId))
            .filter(e -> e.getStatus() == ExecutionStatus.DEAD_LETTERED)
            .orElseThrow(() -> new IllegalArgumentException(
                "Dead-lettered execution not found: " + executionId));

        return ResponseEntity.ok(jobService.manualTrigger(tenantId, deadExec.getJobId()));
    }

    private ExecutionResponse toResponse(JobExecution exec) {
        return ExecutionResponse.builder()
            .id(exec.getId())
            .jobId(exec.getJobId())
            .status(exec.getStatus())
            .attemptNumber(exec.getAttemptNumber())
            .scheduledFireTime(exec.getScheduledFireTime())
            .actualStartTime(exec.getActualStartTime())
            .completedAt(exec.getCompletedAt())
            .fireLagMs(exec.getFireLagMs())
            .nodeId(exec.getNodeId())
            .errorMessage(exec.getErrorMessage())
            .build();
    }
}

