-- V2__node_heartbeats.sql
CREATE TABLE IF NOT EXISTS node_heartbeats (
    node_id VARCHAR(100) PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

