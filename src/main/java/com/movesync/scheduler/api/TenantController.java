package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.TenantResponse;
import com.movesync.scheduler.domain.model.Tenant;
import com.movesync.scheduler.repository.TenantRepository;
import com.movesync.scheduler.service.TenantService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants")
@RequiredArgsConstructor
@Tag(name = "tenant-controller", description = "Tenant Operations & Emergency Controls (Pause, resume, tenant metadata)")
public class TenantController {

    private final TenantService tenantService;
    private final TenantRepository tenantRepository;

    @PostMapping("/{tenantId}/pause")
    @Operation(summary = "Pause tenant", description = "Admin action: halts execution for all tenant jobs. In-memory cached with 5s TTL.")
    public ResponseEntity<Map<String, String>> pauseTenant(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId) {
        tenantService.pauseTenant(tenantId);
        return ResponseEntity.ok(Map.of("status", "PAUSED", "tenantId", tenantId.toString()));
    }

    @PostMapping("/{tenantId}/resume")
    @Operation(summary = "Resume tenant", description = "Admin action: resumes tenant job execution and recovers misfired jobs based on policy.")
    public ResponseEntity<Map<String, String>> resumeTenant(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId) {
        tenantService.resumeTenant(tenantId);
        return ResponseEntity.ok(Map.of("status", "ACTIVE", "tenantId", tenantId.toString()));
    }

    @GetMapping("/{tenantId}")
    @Operation(summary = "Get tenant details", description = "Fetches tenant metadata including configured timezone and current status")
    public ResponseEntity<TenantResponse> getTenant(
            @Parameter(description = "Tenant UUID", example = "11111111-1111-1111-1111-111111111111")
            @PathVariable UUID tenantId) {
        return tenantRepository.findById(tenantId)
            .map(t -> ResponseEntity.ok(TenantResponse.from(t)))
            .orElse(ResponseEntity.notFound().build());
    }
}
