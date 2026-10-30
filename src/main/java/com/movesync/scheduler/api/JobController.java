package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.*;
import com.movesync.scheduler.service.JobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants/{tenantId}/jobs")
@RequiredArgsConstructor
@Tag(name = "job-controller", description = "Job Lifecycle Management (One-off alerts, recurring shift rosters, ad-hoc triggers)")
public class JobController {

    private final JobService jobService;

    @PostMapping
    @Operation(summary = "Create a scheduled job", description = "Schedules a one-off or recurring job with hash-based partition assignment")
    public ResponseEntity<JobResponse> createJob(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                description = "Job creation request payload with demo presets",
                required = true,
                content = @Content(
                    mediaType = "application/json",
                    examples = {
                        @ExampleObject(
                            name = "1. Pickup Alert (One-Off)",
                            summary = "One-off cab pickup alert",
                            value = "{\n" +
                                    "  \"jobType\": \"PICKUP_ALERT\",\n" +
                                    "  \"scheduleType\": \"ONE_OFF\",\n" +
                                    "  \"nextFireTime\": \"2026-10-30T18:30:00Z\",\n" +
                                    "  \"priority\": \"HIGH\",\n" +
                                    "  \"misfirePolicy\": \"FIRE_NOW\",\n" +
                                    "  \"externalRefId\": \"demo-pickup-alert-001\",\n" +
                                    "  \"payload\": \"{\\\"employeeId\\\":\\\"EMP-9821\\\",\\\"cabNumber\\\":\\\"KA-01-MJ-5544\\\"}\"\n" +
                                    "}"
                        ),
                        @ExampleObject(
                            name = "2. Evening Roster (Recurring Cron)",
                            summary = "Daily shift roster generation at 6:00 PM IST",
                            value = "{\n" +
                                    "  \"jobType\": \"ROSTER_GENERATION\",\n" +
                                    "  \"scheduleType\": \"RECURRING\",\n" +
                                    "  \"cronExpression\": \"0 0 18 * * ?\",\n" +
                                    "  \"timezone\": \"Asia/Kolkata\",\n" +
                                    "  \"priority\": \"CRITICAL\",\n" +
                                    "  \"misfirePolicy\": \"FIRE_NOW\",\n" +
                                    "  \"externalRefId\": \"bangalore-evening-roster\",\n" +
                                    "  \"payload\": \"{\\\"shift\\\":\\\"EVENING\\\",\\\"site\\\":\\\"Bangalore-SEZ\\\"}\"\n" +
                                    "}"
                        ),
                        @ExampleObject(
                            name = "3. Pause-Resume Catch-Up Job",
                            summary = "Job configured to test FIRE_NOW misfire catch-up during tenant pause",
                            value = "{\n" +
                                    "  \"jobType\": \"PICKUP_ALERT\",\n" +
                                    "  \"scheduleType\": \"ONE_OFF\",\n" +
                                    "  \"nextFireTime\": \"2026-10-30T12:00:00Z\",\n" +
                                    "  \"priority\": \"HIGH\",\n" +
                                    "  \"misfirePolicy\": \"FIRE_NOW\",\n" +
                                    "  \"externalRefId\": \"paused-catchup-alert-002\",\n" +
                                    "  \"payload\": \"{\\\"message\\\":\\\"Catch-up test after resume\\\"}\"\n" +
                                    "}"
                        )
                    }
                )
            )
            @Valid @RequestBody CreateJobRequest request) {
        JobResponse response = jobService.createJob(tenantId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{jobId}")
    @Operation(summary = "Get job details", description = "Fetches job configuration, partition ID, and current status")
    public ResponseEntity<JobResponse> getJob(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @Parameter(description = "Job UUID")
            @PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.getJob(tenantId, jobId));
    }

    @GetMapping
    @Operation(summary = "List all jobs for tenant", description = "Returns all scheduled and active jobs belonging strictly to the tenant")
    public ResponseEntity<List<JobResponse>> listJobs(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId) {
        return ResponseEntity.ok(jobService.listJobs(tenantId));
    }

    @PostMapping("/{jobId}/cancel")
    @Operation(summary = "Cancel a scheduled job", description = "Cancels future triggers and removes pending executions")
    public ResponseEntity<?> cancelJob(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @Parameter(description = "Job UUID")
            @PathVariable UUID jobId) {
        boolean cancelledExecution = jobService.cancelPendingExecution(tenantId, jobId);
        return ResponseEntity.ok(Map.of(
            "jobStatus", "CANCELLED",
            "pendingExecutionCancelled", cancelledExecution
        ));
    }

    @PostMapping("/{jobId}/trigger")
    @Operation(summary = "Manually trigger job execution", description = "Dispatches job execution immediately, bypassing the timer")
    public ResponseEntity<ExecutionResponse> manualTrigger(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @Parameter(description = "Job UUID")
            @PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.manualTrigger(tenantId, jobId));
    }

    @GetMapping("/{jobId}/next-fire-times")
    @Operation(summary = "Project upcoming fire times", description = "Calculates next N execution timestamps in UTC based on tenant timezone")
    public ResponseEntity<List<Instant>> getNextFireTimes(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId,
            @Parameter(description = "Job UUID")
            @PathVariable UUID jobId,
            @RequestParam(defaultValue = "10") int count) {
        return ResponseEntity.ok(jobService.getNextFireTimes(tenantId, jobId, count));
    }
}
