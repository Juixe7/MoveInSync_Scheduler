package com.movesync.scheduler.exception;

import com.movesync.scheduler.api.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(JobNotFoundException ex, HttpServletRequest req) {
        log.warn("Job not found: {}", ex.getMessage());
        return ResponseEntity.status(404).body(ErrorResponse.builder()
            .status(404).error("Not Found").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(TenantAccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleForbidden(TenantAccessDeniedException ex, HttpServletRequest req) {
        log.warn("Tenant access denied: {}", ex.getMessage());
        return ResponseEntity.status(403).body(ErrorResponse.builder()
            .status(403).error("Forbidden").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(StaleLeaseException.class)
    public ResponseEntity<ErrorResponse> handleStaleLease(StaleLeaseException ex, HttpServletRequest req) {
        log.warn("Stale lease conflict: {}", ex.getMessage());
        // Safe, generic message without internal partition/token IDs
        return ResponseEntity.status(409).body(ErrorResponse.builder()
            .status(409).error("Conflict").message("Lease conflict — please retry later").path(req.getRequestURI()).build());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleBadRequest(IllegalArgumentException ex, HttpServletRequest req) {
        log.warn("Bad request: {}", ex.getMessage());
        return ResponseEntity.status(400).body(ErrorResponse.builder()
            .status(400).error("Bad Request").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(SchedulerException.class)
    public ResponseEntity<ErrorResponse> handleSchedulerException(SchedulerException ex, HttpServletRequest req) {
        log.warn("Scheduler error: {}", ex.getMessage());
        return ResponseEntity.status(400).body(ErrorResponse.builder()
            .status(400).error("Bad Request").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest req) {
        String message = ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .collect(Collectors.joining(", "));
        log.warn("Validation failure: {}", message);
        return ResponseEntity.status(400).body(ErrorResponse.builder()
            .status(400).error("Validation Failed").message(message).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest req) {
        // Log internally, NEVER leak stack trace or PII to client
        log.error("Internal error processing " + req.getRequestURI(), ex);
        return ResponseEntity.status(500).body(ErrorResponse.builder()
            .status(500).error("Internal Error").message("An unexpected error occurred").path(req.getRequestURI()).build());
    }
}
