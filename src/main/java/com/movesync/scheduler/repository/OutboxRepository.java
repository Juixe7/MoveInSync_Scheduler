package com.movesync.scheduler.repository;

import com.movesync.scheduler.domain.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query(value = """
        SELECT * FROM outbox_events
        WHERE published = false
        ORDER BY created_at ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<OutboxEvent> findUnpublished(@Param("limit") int limit);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.published = true WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids);
}

