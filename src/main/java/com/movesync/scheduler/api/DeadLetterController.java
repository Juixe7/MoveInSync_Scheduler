package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.ExecutionResponse;
import com.movesync.scheduler.domain.model.ExecutionStatus;
import com.movesync.scheduler.domain.model.JobExecution;
import com.movesync.scheduler.repository.JobExecutionRepository;
import com.movesync.scheduler.service.JobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants/{tenantId}/dead-letter")
@RequiredArgsConstructor
@Tag(name = "dead-letter-controller", description = "Dead-Letter Queue & Manual Replay (Observability and recovery for failed jobs)")
public class DeadLetterController {

    private final JobExecutionRepository executionRepository;
    private final JobService jobService;

    @GetMapping
    @Operation(summary = "List dead-lettered jobs", description = "Returns all failed job executions that have exhausted retries")
    public ResponseEntity<List<ExecutionResponse>> listDeadLettered(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
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
    @Operation(summary = "Replay dead-lettered job", description = "Manually retries a failed execution by re-triggering the job")
    public ResponseEntity<?> replay(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @Parameter(description = "Execution UUID")
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
