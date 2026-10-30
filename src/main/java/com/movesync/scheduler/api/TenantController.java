package com.movesync.scheduler.api;

import com.movesync.scheduler.api.dto.TenantResponse;
import com.movesync.scheduler.domain.model.Tenant;
import com.movesync.scheduler.repository.TenantRepository;
import com.movesync.scheduler.service.TenantService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/tenants")
@RequiredArgsConstructor
public class TenantController {

    private final TenantService tenantService;
    private final TenantRepository tenantRepository;

    @PostMapping("/{tenantId}/pause")
    public ResponseEntity<Map<String, String>> pauseTenant(@PathVariable UUID tenantId) {
        tenantService.pauseTenant(tenantId);
        return ResponseEntity.ok(Map.of("status", "PAUSED", "tenantId", tenantId.toString()));
    }

    @PostMapping("/{tenantId}/resume")
    public ResponseEntity<Map<String, String>> resumeTenant(@PathVariable UUID tenantId) {
        tenantService.resumeTenant(tenantId);
        return ResponseEntity.ok(Map.of("status", "ACTIVE", "tenantId", tenantId.toString()));
    }

    @GetMapping("/{tenantId}")
    public ResponseEntity<TenantResponse> getTenant(@PathVariable UUID tenantId) {
        return tenantRepository.findById(tenantId)
            .map(t -> ResponseEntity.ok(TenantResponse.from(t)))
            .orElse(ResponseEntity.notFound().build());
    }
}

