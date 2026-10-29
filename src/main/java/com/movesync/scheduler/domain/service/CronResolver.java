package com.movesync.scheduler.domain.service;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CronResolver {

    private final CronParser parser;

    public CronResolver() {
        this.parser = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
        );
    }

    /**
     * Compute the next fire time after 'after' in the given timezone.
     * Returns UTC instant.
     *
     * DST spring-forward: if the local time doesn't exist (e.g., 02:30 during
     * spring-forward), ZonedDateTime advances to the next valid time (03:00).
     *
     * DST fall-back: if the local time occurs twice, we use the first
     * occurrence (withEarlierOffsetAtOverlap is the default in java.time).
     */
    public Instant nextFireTime(String cronExpression, String timezoneId, Instant after) {
        Cron cron = parser.parse(cronExpression);
        cron.validate();

        ZoneId zone = ZoneId.of(timezoneId);
        ZonedDateTime zonedAfter = after.atZone(zone);

        ExecutionTime executionTime = ExecutionTime.forCron(cron);
        Optional<ZonedDateTime> next = executionTime.nextExecution(zonedAfter);

        return next.orElseThrow(() -> new IllegalArgumentException(
            "No next execution for cron: " + cronExpression
        )).toInstant();
    }

    /**
     * Compute the next N fire times (for the "list next fire times" API).
     */
    public List<Instant> nextFireTimes(String cronExpression, String timezoneId,
                                        Instant after, int count) {
        List<Instant> times = new ArrayList<>();
        Instant current = after;
        for (int i = 0; i < count; i++) {
            current = nextFireTime(cronExpression, timezoneId, current);
            times.add(current);
        }
        return times;
    }

    /**
     * Validate a cron expression. Throws if invalid.
     */
    public void validate(String cronExpression) {
        parser.parse(cronExpression).validate();
    }
}

