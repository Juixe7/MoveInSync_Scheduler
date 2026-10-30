package com.movesync.scheduler.exception;

import com.movesync.scheduler.api.dto.ErrorResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler exceptionHandler;
    private HttpServletRequest request;

    @BeforeEach
    void setUp() {
        exceptionHandler = new GlobalExceptionHandler();
        request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/api/test");
    }

    @Test
    @DisplayName("Should return 404 for JobNotFoundException")
    void shouldHandleJobNotFound() {
        UUID jobId = UUID.randomUUID();
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleNotFound(
            new JobNotFoundException(jobId), request);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getError()).isEqualTo("Not Found");
        assertThat(response.getBody().getMessage()).contains(jobId.toString());
    }

    @Test
    @DisplayName("Should return 409 Conflict with generic message for StaleLeaseException")
    void shouldHandleStaleLeaseWithoutInternalLeaks() {
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleStaleLease(
            new StaleLeaseException(5, 42L), request);

        assertThat(response.getStatusCode().value()).isEqualTo(409);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getError()).isEqualTo("Conflict");
        // Must be safe generic message
        assertThat(response.getBody().getMessage()).isEqualTo("Lease conflict — please retry later");
        assertThat(response.getBody().getMessage()).doesNotContain("partition 5");
    }

    @Test
    @DisplayName("Should return 400 for IllegalArgumentException")
    void shouldHandleBadRequest() {
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleBadRequest(
            new IllegalArgumentException("Invalid schedule parameter"), request);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getError()).isEqualTo("Bad Request");
        assertThat(response.getBody().getMessage()).isEqualTo("Invalid schedule parameter");
    }

    @Test
    @DisplayName("Should return 500 without stack trace for unexpected Exception")
    void shouldHandleGenericExceptionSafely() {
        ResponseEntity<ErrorResponse> response = exceptionHandler.handleGeneric(
            new RuntimeException("Critical DB failure: table corrupted"), request);

        assertThat(response.getStatusCode().value()).isEqualTo(500);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getError()).isEqualTo("Internal Error");
        assertThat(response.getBody().getMessage()).isEqualTo("An unexpected error occurred");
        assertThat(response.getBody().getMessage()).doesNotContain("Critical DB failure");
    }
}

