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

    private final CronParser unixParser;
    private final CronParser quartzParser;
    private final CronParser springParser;

    public CronResolver() {
        this.unixParser = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
        );
        this.quartzParser = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
        );
        this.springParser = new CronParser(
            CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53)
        );
    }

    private Cron parseCron(String cronExpression) {
        String trimmed = cronExpression.trim();
        String[] parts = trimmed.split("\\s+");
        if (parts.length == 5) {
            return unixParser.parse(trimmed);
        } else if (parts.length == 6 || parts.length == 7) {
            try {
                return quartzParser.parse(trimmed);
            } catch (Exception e) {
                return springParser.parse(trimmed);
            }
        }
        return unixParser.parse(trimmed);
    }

    public Instant nextFireTime(String cronExpression, String timezoneId) {
        return nextFireTime(cronExpression, timezoneId, Instant.now());
    }

    /**
     * Compute the next fire time after 'after' in the given timezone.
     * Returns UTC instant.
     */
    public Instant nextFireTime(String cronExpression, String timezoneId, Instant after) {
        Cron cron = parseCron(cronExpression);
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
        parseCron(cronExpression).validate();
    }
}
