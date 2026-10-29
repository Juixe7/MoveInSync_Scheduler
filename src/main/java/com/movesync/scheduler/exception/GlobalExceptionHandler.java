package com.movesync.scheduler.exception;

import com.movesync.scheduler.api.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(JobNotFoundException ex, HttpServletRequest req) {
        return ResponseEntity.status(404).body(ErrorResponse.builder()
            .status(404).error("Not Found").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(TenantAccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleForbidden(TenantAccessDeniedException ex, HttpServletRequest req) {
        return ResponseEntity.status(403).body(ErrorResponse.builder()
            .status(403).error("Forbidden").message(ex.getMessage()).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest req) {
        String message = ex.getBindingResult().getFieldErrors().stream()
            .map(e -> e.getField() + ": " + e.getDefaultMessage())
            .collect(Collectors.joining(", "));
        return ResponseEntity.status(400).body(ErrorResponse.builder()
            .status(400).error("Validation Failed").message(message).path(req.getRequestURI()).build());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest req) {
        // NEVER leak stack trace or PII
        return ResponseEntity.status(500).body(ErrorResponse.builder()
            .status(500).error("Internal Error").message("An unexpected error occurred").path(req.getRequestURI()).build());
    }
}

