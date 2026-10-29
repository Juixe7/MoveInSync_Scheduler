package com.movesync.scheduler.api.dto;

import lombok.*;
import java.time.Instant;

@Getter @Setter
@NoArgsConstructor @AllArgsConstructor
@Builder
public class ErrorResponse {
    private int status;
    private String error;
    private String message;     // meaningful but no PII
    private String path;
    @Builder.Default
    private Instant timestamp = Instant.now();
}

