package com.movesync.scheduler.api.dto;

import com.movesync.scheduler.domain.model.Tenant;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class TenantResponse {
    private UUID id;
    private String name;
    private Tenant.TenantStatus status;
    private String timezone;
    private Instant pausedAt;
    private Instant createdAt;

    public static TenantResponse from(Tenant tenant) {
        if (tenant == null) return null;
        return TenantResponse.builder()
            .id(tenant.getId())
            .name(tenant.getName())
            .status(tenant.getStatus())
            .timezone(tenant.getTimezone())
            .pausedAt(tenant.getPausedAt())
            .createdAt(tenant.getCreatedAt())
            .build();
    }
}
