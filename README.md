# MoveInSync Distributed Trip Job Scheduler

A production-grade, fault-tolerant, multi-tenant distributed job scheduler built for MoveInSync's 24/7 employee transport platform. Built with **Spring Boot 4.1.2**, **Java 21**, **PostgreSQL 16**, and **Kafka 3.8 (KRaft)**.

Designed to coordinate hundreds of thousands of daily time-critical jobs (roster generation, cab pickup alerts, no-show cancellations, billing reconciliation) across a 3-node cluster with exactly-once execution effects, automatic failover, and tenant fairness.

---

## Demo Video

- **Video Walkthrough (Google Drive):** [MoveInSync Scheduler Demo & Chaos Kill Test](https://drive.google.com/drive/folders/1EuAMylkT7YqvRZ7Ju1IkQQkO2z2Y0kBC)

---

## Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │          Load Balancer / Ingress       │
                      │  Swagger UI: /swagger-ui.html          │
                      └───────┬───────────┬───────────┬────────┘
                              │           │           │
                     ┌────────▼───┐  ┌────▼───────┐  ┌▼───────────┐
                     │ Scheduler  │  │ Scheduler  │  │ Scheduler  │
                     │   Node 1   │  │   Node 2   │  │   Node 3   │
                     │ (port 8081)│  │ (port 8082)│  │ (port 8083)│
                     └────────┬───┘  └────┬───────┘  └┬───────────┘
                              │           │           │
                              └───────────┼───────────┘
                                          │
                              ┌───────────▼───────────┐
                              │  PostgreSQL (Shared)  │
                              │  - 64 Partition Leases│
                              │  - Fencing Tokens     │
                              │  - Job Executions     │
                              │  - Outbox Events Table│
                              └───────────┬───────────┘
                                          │
                               ┌──────────▼───────────┐
                               │     Apache Kafka     │
                               │  (Outbox Event Bus)  │
                               └──────────────────────┘
```

### Key Engineering Guarantees

| Guarantee | Mechanism | Verification |
|---|---|---|
| **Zero Job Loss (G1)** | Persistent state machine + failover misfire scanning | 100,000 / 100,000 jobs completed |
| **No Early Firing (G2)** | `next_fire_time <= NOW()` index predicate on DB clock | Proven in unit & integration tests |
| **Idempotent Creation (G3)** | Deterministic idempotency key `{jobId}:{scheduledFireTime}` | 0 duplicate executions under load |
| **Zombie Protection (G4)** | Monotonic fencing token CAS checked on completion | Stale writes rejected (`FENCING_REJECTED`) |
| **Multi-Tenant Fairness (G5)** | Per-tenant in-memory `FairnessQueue` semaphores | No tenant starves others |
| **Full Coverage (G6)** | DB leases with `SKIP LOCKED` and self-healing heartbeat | All 64 partitions covered |
| **Transactional Outbox (G7)** | Events written atomically with execution status in PostgreSQL | Publisher delivers to Kafka with dedup |
| **DB-Clock Leases (G8)** | Leases computed via PostgreSQL `NOW()` (30s timeout) | Immune to JVM NTP drift |
| **Cancel-vs-Fire Race (G9)** | Two-step atomic transition on `status = 'PENDING'` | Zero conflicting states |

---

## Live 100K Chaos Benchmark Results

A full 100,000-job chaos benchmark was executed against a live 3-node cluster with container kill (`kill -9`) and container freeze (`docker pause` 35s):

| Metric | Target | Actual Benchmark Result | Status |
|---|---|---|---|
| **Total Jobs Processed** | 100,000 | **100,000** | ✅ 100% (Zero Loss) |
| **Duplicate Executions** | 0 | **0** | ✅ Exactly-Once Effect |
| **Steady-State Fire Lag (p50)** | < 1,000 ms | **189.10 ms** | ✅ Exceeded |
| **Steady-State Fire Lag (p95)** | < 1,500 ms | **437.56 ms** | ✅ Exceeded |
| **Steady-State Fire Lag (p99)** | < 2,000 ms | **814.62 ms** | ✅ Exceeded (< 1s) |
| **Post-Recovery Fire Lag (p99)**| < 5,000 ms | **1,074.76 ms** | ✅ Exceeded |
| **Zombie Fencing Rejections** | $\ge 1$ | **$\ge 7$ Verified** | ✅ Stale Writes Fenced |

> **Benchmark Configuration Note:**
> The benchmark used elevated pool sizes (50 worker threads, batch size 150) configured via Docker Compose environment variables to ensure sufficient single-node headroom during chaos events (when Node 2 was killed and Node 3 was frozen). Production defaults (20 worker threads, batch size 50) configured in `application.yml` are appropriate for steady-state 3-node operation.

---

## Interactive API & Swagger Documentation

Interactive Swagger documentation is available out-of-the-box for interview evaluation:

- **Swagger UI:** `http://localhost:8080/swagger-ui.html`
- **OpenAPI JSON Docs:** `http://localhost:8080/v3/api-docs`
- **Prometheus Metrics:** `http://localhost:8080/actuator/prometheus`
- **Health Check:** `http://localhost:8080/actuator/health`

### Authentication (Tenant-Scoped RBAC)

On startup, `SeedTokenPrinter` outputs pre-signed JWT Bearer tokens to the logs for easy copy-pasting into Swagger UI or cURL:

```bash
# Example API Call using Generated Admin Token
curl -X POST http://localhost:8080/api/jobs \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "11111111-1111-1111-1111-111111111111",
    "jobType": "PICKUP_ALERT",
    "scheduleType": "ONE_OFF",
    "priority": "HIGH",
    "scheduledFireTime": "2026-10-30T12:00:00Z"
  }'
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/api/tenants/{tenantId}/jobs` | Create a job |
| GET | `/api/tenants/{tenantId}/jobs` | List tenant's jobs |
| GET | `/api/tenants/{tenantId}/jobs/{jobId}` | Get job details |
| POST | `/api/tenants/{tenantId}/jobs/{jobId}/cancel` | Cancel a job |
| POST | `/api/tenants/{tenantId}/jobs/{jobId}/trigger` | Manual trigger |
| GET | `/api/tenants/{tenantId}/jobs/{jobId}/next-fire-times` | Next N fire times |
| POST | `/api/tenants/{tenantId}/pause` | Pause all tenant jobs |
| POST | `/api/tenants/{tenantId}/resume` | Resume tenant jobs |
| GET | `/api/tenants/{tenantId}/dead-letter` | List dead-lettered runs |
| POST | `/api/tenants/{tenantId}/dead-letter/{execId}/replay` | Replay a dead-letter |
| GET | `/actuator/health` | Health check |
| GET | `/actuator/prometheus` | Prometheus metrics |
| GET | `/swagger-ui.html` | Swagger UI |

---

## How to Run

### Option 1: 3-Node Cluster via Docker Compose (Recommended)

```powershell
# Start PostgreSQL, Kafka, and 3 scheduler nodes
docker compose up --build -d

# Check cluster logs and test JWT tokens
docker compose logs -f scheduler-node1

# Verify all 3 nodes have claimed partitions
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT node_id, count(*) FROM partition_leases GROUP BY node_id;"
```

### Option 2: Single Node for Local Development

```powershell
# Set Java 21 environment
$env:JAVA_HOME = "C:\Users\Asus\.jdks\temurin-21"

# Run tests
.\mvnw.cmd test

# Run application locally
.\mvnw.cmd spring-boot:run
```

---

## Memory & Resource Recommendations

| Component | Minimum | Recommended Production | Notes |
|---|---|---|---|
| **JVM Heap (`-Xms / -Xmx`)** | 512 MB | **1 GB – 2 GB** | Keeps GC pause times under 50ms during high-throughput polling |
| **HikariCP Connection Pool** | 10 conns | **20 – 60 conns / node** | Matches worker pool concurrency + poll threads |
| **PostgreSQL `max_connections`** | 100 | **$\ge 300$** | Accommodates 3 nodes with elevated pools during peak load |
| **PostgreSQL Work Memory** | 16 MB | **64 MB** | Accelerates index scans on `idx_jobs_partition_poll` |

---

## Future Improvements

1. **Elastic Fairness (Burst Mode):** 
   Currently, `FairnessQueue` enforces a fixed `maxConcurrentPerTenant = 50` semaphore cap. A future enhancement can dynamically allow tenants to burst up to 100% of available worker threads when total cluster utilization is below 30%, scaling back gracefully as other tenants become active.
2. **Adaptive Polling with Exponential Backoff:** 
   Currently, partitions are polled every 500ms. When a node detects consecutive empty poll cycles on its owned partitions, it can back off exponentially up to 5,000ms, immediately waking up via PostgreSQL `LISTEN / NOTIFY` or job insertion events.
3. **Time-Partitioned `job_executions` Tables:** 
   In production with millions of jobs per month, declarative PostgreSQL table partitioning (by month or week) on `job_executions` will enable instantaneous zero-downtime drops of historical data without running heavy `DELETE` or `VACUUM` operations.
4. **Priority-Aware Poll Index:** 
   Extending the poll index to `(partition_id, status, priority, next_fire_time)` will allow PostgreSQL to return `CRITICAL` priority jobs directly from the B-tree index in sort order, removing the need for in-memory sorting.

---

## In-Depth Documentation

- [Demo Walkthrough & Failover Video](https://drive.google.com/drive/folders/1EuAMylkT7YqvRZ7Ju1IkQQkO2z2Y0kBC) — complete walkthrough demonstrating job execution, tenant isolation, and live node kill failover.
- [Detailed Architecture & Fencing Explanation](file:///C:/Users/Asus/Desktop/scheduler/EXPLANATION.md) — complete deep-dive covering failure modes, zombie fencing diagram, Two Generals Problem, cancel race, and Big-O tables.
- [Design Decisions Note](file:///C:/Users/Asus/Desktop/scheduler/DESIGN_NOTE.md) — why `hash(jobId)` was chosen over `hash(tenantId)`, lease design, and outbox pattern.
- [Engineering Trade-Offs](file:///C:/Users/Asus/Desktop/scheduler/TRADE_OFFS.md) — explicit documentation of intentional trade-offs and operational limits.

