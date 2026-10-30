package com.movesync.scheduler.engine;

import com.movesync.scheduler.domain.model.OutboxEvent;
import com.movesync.scheduler.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "scheduler.events";
    private static final int BATCH_SIZE = 100;

    /**
     * Polls outbox every 1 second, publishes to Kafka, marks published.
     * Uses @Scheduled instead of manual executor for simplicity.
     * Spring manages the thread and transaction.
     */
    @Scheduled(fixedDelayString = "${scheduler.outbox-poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxRepository.findUnpublished(BATCH_SIZE);
        if (events.isEmpty()) return;

        List<UUID> publishedIds = new ArrayList<>();

        for (OutboxEvent event : events) {
            try {
                // Use aggregate ID as Kafka key for partition ordering
                kafkaTemplate.send(TOPIC, event.getAggregateId().toString(), event.getPayload());
                publishedIds.add(event.getId());
            } catch (Exception e) {
                // If Kafka is down, stop this batch. Events stay unpublished.
                // They'll be retried on the next poll (1 second later).
                log.warn("Kafka publish failed, {} events remain unpublished: {}",
                    events.size() - publishedIds.size(), e.getMessage());
                break;
            }
        }

        if (!publishedIds.isEmpty()) {
            outboxRepository.markPublished(publishedIds);
            log.debug("Published {} events to Kafka", publishedIds.size());
        }
    }
}

