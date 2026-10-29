package com.movesync.scheduler.domain.service;

import java.util.concurrent.ThreadLocalRandom;

public class RetryCalculator {

    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final double multiplier;
    private final double jitterFactor;

    public RetryCalculator(long initialBackoffMs, long maxBackoffMs,
                           double multiplier, double jitterFactor) {
        this.initialBackoffMs = initialBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.multiplier = multiplier;
        this.jitterFactor = jitterFactor;
    }

    /**
     * Compute backoff for the given attempt number (1-based).
     * attempt 1 → initialBackoff
     * attempt 2 → initialBackoff * multiplier
     * attempt N → initialBackoff * multiplier^(N-1), capped at maxBackoff
     * Then apply ± jitter.
     */
    public long computeBackoffMs(int attempt) {
        double base = initialBackoffMs * Math.pow(multiplier, attempt - 1);
        base = Math.min(base, maxBackoffMs);

        // Apply jitter: ± jitterFactor
        double jitter = base * jitterFactor;
        double min = base - jitter;
        double max = base + jitter;

        return (long) ThreadLocalRandom.current().nextDouble(min, max);
    }

    public boolean shouldRetry(int currentAttempt, int maxRetries) {
        return currentAttempt < maxRetries;
    }
}

