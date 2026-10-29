package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.*;
import com.movesync.scheduler.service.JobService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants/{tenantId}/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping
    public ResponseEntity<JobResponse> createJob(
            @PathVariable UUID tenantId,
            @Valid @RequestBody CreateJobRequest request) {
        JobResponse response = jobService.createJob(tenantId, request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobResponse> getJob(
            @PathVariable UUID tenantId,
            @PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.getJob(tenantId, jobId));
    }

    @GetMapping
    public ResponseEntity<List<JobResponse>> listJobs(@PathVariable UUID tenantId) {
        return ResponseEntity.ok(jobService.listJobs(tenantId));
    }

    @PostMapping("/{jobId}/cancel")
    public ResponseEntity<JobResponse> cancelJob(
            @PathVariable UUID tenantId,
            @PathVariable UUID jobId) {
        return ResponseEntity.ok(jobService.cancelJob(tenantId, jobId));
    }

    @GetMapping("/{jobId}/next-fire-times")
    public ResponseEntity<List<Instant>> getNextFireTimes(
            @PathVariable UUID tenantId,
            @PathVariable UUID jobId,
            @RequestParam(defaultValue = "10") int count) {
        return ResponseEntity.ok(jobService.getNextFireTimes(tenantId, jobId, count));
    }
}

