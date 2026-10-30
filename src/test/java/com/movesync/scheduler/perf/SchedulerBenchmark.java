package com.movesync.scheduler.perf;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * 100K Job Chaos Benchmark Runner
 *
 * Ingests 100,000 jobs spread across 10 tenants and 64 partitions.
 * Calculates throughput, completion rate, duplicate check, and fire lag percentiles (p50, p90, p95, p99).
 */
public class SchedulerBenchmark {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/scheduler_db?rewriteBatchedStatements=true";
    private static final String DB_USER = "scheduler";
    private static final String DB_PASS = "scheduler_pass";

    private static final int TOTAL_JOBS = 100_000;
    private static final int BATCH_SIZE = 2_500;
    private static final int TOTAL_PARTITIONS = 64;
    private static final int SPREAD_WINDOW_SECONDS = 600; // 10 minutes

    public static void main(String[] args) throws Exception {
        System.out.println("=================================================================");
        System.out.println("  100K Distributed Job Chaos Benchmark");
        System.out.println("=================================================================");

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            conn.setAutoCommit(false);

            List<UUID> tenantIds = setupTenants(conn);
            cleanPreviousRuns(conn);
            insertJobsBatch(conn, tenantIds);
        }

        System.out.println("\n[BENCHMARK] Ingestion complete. Cluster is processing 100k jobs.");
    }

    public static List<UUID> setupTenants(Connection conn) throws SQLException {
        List<UUID> tenantIds = new ArrayList<>();
        String[] timezones = {"Asia/Kolkata", "America/New_York", "Europe/London", "UTC", "Asia/Tokyo"};

        String sql = """
            INSERT INTO tenants (id, name, timezone, status, created_at, updated_at)
            VALUES (?, ?, ?, 'ACTIVE', NOW(), NOW())
            ON CONFLICT (name) DO UPDATE SET status = 'ACTIVE'
            RETURNING id;
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 10; i++) {
                UUID tid = UUID.nameUUIDFromBytes(("benchmark-tenant-" + i).getBytes());
                ps.setObject(1, tid);
                ps.setString(2, "bench-tenant-" + i);
                ps.setString(3, timezones[i % timezones.length]);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        tenantIds.add((UUID) rs.getObject(1));
                    }
                }
            }
        }
        conn.commit();
        System.out.printf("[BENCHMARK] Configured %d benchmark tenants.%n", tenantIds.size());
        return tenantIds;
    }

    public static void cleanPreviousRuns(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DELETE FROM job_executions WHERE tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');");
            stmt.executeUpdate("DELETE FROM jobs WHERE tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');");
            stmt.executeUpdate("DELETE FROM outbox_events;");
        }
        conn.commit();
        System.out.println("[BENCHMARK] Cleaned previous benchmark executions and jobs.");
    }

    public static void insertJobsBatch(Connection conn, List<UUID> tenantIds) throws SQLException {
        System.out.printf("[BENCHMARK] Ingesting %d jobs via JDBC batching (batch size %d)...%n", TOTAL_JOBS, BATCH_SIZE);
        long startTime = System.currentTimeMillis();

        String insertSql = """
            INSERT INTO jobs (
                id, tenant_id, external_ref_id, job_type, priority, schedule_type,
                next_fire_time, payload, status, misfire_policy, max_retries,
                max_run_time_sec, partition_id, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, 'ONE_OFF',
                ?, CAST(? AS jsonb), 'ACTIVE', 'FIRE_NOW', 3,
                300, ?, NOW(), NOW()
            );
            """;

        Instant baseTime = Instant.now();
        Random rng = new Random(42);

        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (int i = 0; i < TOTAL_JOBS; i++) {
                UUID jobId = UUID.randomUUID();
                UUID tenantId = tenantIds.get(i % tenantIds.size());

                // Job type distribution: 70% PICKUP_ALERT, 15% ROSTER_GENERATION, 10% NO_SHOW_CANCEL, 5% BILLING_CLOSE
                String jobType;
                String priority;
                int typeRoll = i % 100;
                if (typeRoll < 70) {
                    jobType = "PICKUP_ALERT";
                    priority = "HIGH";
                } else if (typeRoll < 85) {
                    jobType = "ROSTER_GENERATION";
                    priority = "NORMAL";
                } else if (typeRoll < 95) {
                    jobType = "NO_SHOW_CANCEL";
                    priority = "HIGH";
                } else {
                    jobType = "BILLING_CLOSE";
                    priority = "CRITICAL";
                }

                // Spread fire times evenly over 10 minutes (600 seconds)
                int offsetSec = (int) (((long) i * SPREAD_WINDOW_SECONDS) / TOTAL_JOBS);
                Instant fireTime = baseTime.plusSeconds(offsetSec);

                // Deterministic partition hash
                int partition = Math.abs(jobId.hashCode()) % TOTAL_PARTITIONS;

                ps.setObject(1, jobId);
                ps.setObject(2, tenantId);
                ps.setString(3, "bench-ref-" + i);
                ps.setString(4, jobType);
                ps.setString(5, priority);
                ps.setTimestamp(6, Timestamp.from(fireTime));
                ps.setString(7, String.format("{\"seq\":%d,\"type\":\"%s\"}", i, jobType));
                ps.setInt(8, partition);

                ps.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    ps.executeBatch();
                    conn.commit();
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        double rate = (TOTAL_JOBS / (duration / 1000.0));
        System.out.printf("[BENCHMARK] Ingested %d jobs in %d ms (%.1f jobs/sec)%n", TOTAL_JOBS, duration, rate);
    }

    public static BenchmarkMetrics fetchMetrics(Connection conn) throws SQLException {
        BenchmarkMetrics m = new BenchmarkMetrics();
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM jobs WHERE tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');")) {
                if (rs.next()) m.totalJobs = rs.getInt(1);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM jobs WHERE status = 'COMPLETED' AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');")) {
                if (rs.next()) m.completedJobs = rs.getInt(1);
            }
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM jobs WHERE status = 'ACTIVE' AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');")) {
                if (rs.next()) m.activeJobs = rs.getInt(1);
            }
            try (ResultSet rs = stmt.executeQuery("""
                SELECT 
                  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY fire_lag_ms) AS p50,
                  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fire_lag_ms) AS p90,
                  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fire_lag_ms) AS p95,
                  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY fire_lag_ms) AS p99,
                  MIN(fire_lag_ms) AS min_lag,
                  MAX(fire_lag_ms) AS max_lag
                FROM job_executions
                WHERE status = 'COMPLETED'
                  AND fire_lag_ms IS NOT NULL
                  AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%');
                """)) {
                if (rs.next()) {
                    m.p50LagMs = rs.getDouble("p50");
                    m.p90LagMs = rs.getDouble("p90");
                    m.p95LagMs = rs.getDouble("p95");
                    m.p99LagMs = rs.getDouble("p99");
                    m.minLagMs = rs.getLong("min_lag");
                    m.maxLagMs = rs.getLong("max_lag");
                }
            }
            try (ResultSet rs = stmt.executeQuery("""
                SELECT count(*) FROM (
                  SELECT job_id FROM job_executions 
                  WHERE status = 'COMPLETED'
                    AND tenant_id IN (SELECT id FROM tenants WHERE name LIKE 'bench-tenant-%')
                  GROUP BY job_id HAVING count(*) > 1
                ) dupes;
                """)) {
                if (rs.next()) m.duplicateExecutions = rs.getInt(1);
            }
        }
        return m;
    }

    public static class BenchmarkMetrics {
        public int totalJobs;
        public int completedJobs;
        public int activeJobs;
        public int duplicateExecutions;
        public double p50LagMs;
        public double p90LagMs;
        public double p95LagMs;
        public double p99LagMs;
        public long minLagMs;
        public long maxLagMs;
    }
}
