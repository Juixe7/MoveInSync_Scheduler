# MoveInSync Distributed Trip Job Scheduler

A production-grade, fault-tolerant, multi-tenant distributed job scheduler engineered for MoveInSync's 24/7 employee transport platform. Built with **Spring Boot 4.1.2**, **Java 21**, **PostgreSQL 16**, and **Kafka 3.8 (KRaft)**.

Designed to coordinate hundreds of thousands of daily time-critical jobs (roster generation, cab pickup alerts, no-show cancellations, billing reconciliation) across a 3-node cluster with exactly-once execution effects, automatic failover, and multi-tenant fairness.

---

## Quick Reference Links

- **Demo Video Walkthrough:** [MoveInSync Scheduler Demo & Chaos Kill Test (Google Drive)](https://drive.google.com/drive/folders/1EuAMylkT7YqvRZ7Ju1IkQQkO2z2Y0kBC)
- **Interactive Swagger UI:** `http://localhost:8080/swagger-ui.html`
- **Prometheus Metrics:** `http://localhost:8080/actuator/prometheus`
- **Detailed Architecture & Fencing Deep-Dive:** [EXPLANATION.md](file:///c:/Users/Asus/Desktop/scheduler/EXPLANATION.md)
- **Design Decisions Note:** [DESIGN_NOTE.md](file:///c:/Users/Asus/Desktop/scheduler/DESIGN_NOTE.md)
- **Engineering Trade-Offs:** [TRADE_OFFS.md](file:///c:/Users/Asus/Desktop/scheduler/TRADE_OFFS.md)
- **Phased Implementation Roadmap:** [commit_plan.md](file:///c:/Users/Asus/Desktop/scheduler/commit_plan.md)

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

### Execution Sequence & Lifecycle

- **Scheduler Job Execution Sequence:** Demonstrates how scheduler nodes poll owned partitions via `SKIP LOCKED`, dispatch due jobs through tenant semaphores, enforce CAS fencing on completion, and write outbox events.
  
  ![Scheduler job execution sequence](docs/images/scheduler-sequence-diagram.png)

- **Job Execution State Lifecycle:** Details state transitions (`PENDING` -> `RUNNING` -> `COMPLETED` / `FAILED` / `MISFIRED` / `DEAD_LETTER` / `FENCING_REJECTED`) and cancel-vs-fire race resolution.
  
  ![Job execution lifecycle](docs/images/job-execution-lifecycle.png)

---

## Live 100K Chaos Benchmark Results

A full 100,000-job chaos benchmark was executed against a live 3-node cluster with container kill (`kill -9`) and container freeze (`docker pause` 35s):

| Metric | Target | Actual Benchmark Result | Status |
|---|---|---|---|
| **Total Jobs Processed** | 100,000 | **100,000** | ✅ 100% (Zero Job Loss) |
| **Duplicate Executions** | 0 | **0** | ✅ Exactly-Once Effect |
| **Steady-State Fire Lag (p50)** | < 1,000 ms | **189.10 ms** | ✅ Exceeded |
| **Steady-State Fire Lag (p95)** | < 1,500 ms | **437.56 ms** | ✅ Exceeded |
| **Steady-State Fire Lag (p99)** | < 2,000 ms | **814.62 ms** | ✅ Exceeded (< 1s) |
| **Post-Recovery Fire Lag (p99)**| < 5,000 ms | **1,074.76 ms** | ✅ Exceeded |
| **Zombie Fencing Rejections** | $\ge 1$ | **$\ge 7$ Verified** | ✅ Stale Writes Fenced |

> **Benchmark Configuration Note:**
> The benchmark used elevated pool sizes (50 worker threads, batch size 150) configured via Docker Compose environment variables to ensure sufficient single-node headroom during chaos events (when Node 2 was killed and Node 3 was frozen). Production defaults (20 worker threads, batch size 50) configured in `application.yml` are appropriate for steady-state 3-node operation.

---

## Complete Repository Structure & File Map

Below is the complete, annotated directory structure mapping every file, configuration, script, source package, and test suite in this repository:

```
scheduler/
├── .mvn/
│   └── wrapper/
│       └── maven-wrapper.properties            # Maven Wrapper configuration (Maven 3.9+)
├── docs/
│   └── images/
│       ├── job-execution-lifecycle.png         # State transition diagram (retries, misfires, dead letter)
│       └── scheduler-sequence-diagram.png      # End-to-end execution sequence diagram
├── src/
│   ├── main/
│   │   ├── java/com/movesync/scheduler/
│   │   │   ├── SchedulerApplication.java       # Spring Boot main entry point
│   │   │   │
│   │   │   ├── api/                            # REST API Presentation Layer
│   │   │   │   ├── JobController.java          # Job CRUD, cancel, manual trigger, next-fire-times
│   │   │   │   ├── TenantController.java       # Tenant pause & resume operations
│   │   │   │   ├── DeadLetterController.java   # Dead-letter queue inspection and manual replay
│   │   │   │   └── dto/                        # Transfer objects with validation annotations
│   │   │   │       ├── CreateJobRequest.java   # Payload for job creation (CRON, one-off, priority)
│   │   │   │       ├── JobResponse.java        # Read projection of Job entity
│   │   │   │       ├── ExecutionResponse.java  # Historical execution projection
│   │   │   │       ├── TenantResponse.java     # Tenant state projection
│   │   │   │       └── ErrorResponse.java      # Standard RFC 7807-style API error response
│   │   │   │
│   │   │   ├── config/                         # Infrastructure & Spring Configuration
│   │   │   │   ├── BeanConfig.java             # Thread pools (critical & worker), Caffeine cache, JSON mapper
│   │   │   │   ├── KafkaProducerConfig.java    # Kafka producer with idempotence (enable.idempotence=true)
│   │   │   │   ├── OpenApiConfig.java          # Swagger 3.0 / OpenAPI documentation bean
│   │   │   │   ├── SchedulerProperties.java    # Type-safe @ConfigurationProperties (pools, batch, leases)
│   │   │   │   ├── SchedulingConfig.java       # Spring @EnableScheduling configuration
│   │   │   │   ├── SecurityConfig.java         # Stateless Spring Security filter chain & CORS
│   │   │   │   └── SecurityProperties.java     # JWT secret keys, issuer, and token TTL settings
│   │   │   │
│   │   │   ├── domain/                         # Domain Entities & Core Algorithmic Services
│   │   │   │   ├── model/                      # JPA Entities and Enums
│   │   │   │   │   ├── ExecutionStatus.java    # PENDING, RUNNING, COMPLETED, FAILED, MISFIRED, etc.
│   │   │   │   │   ├── Job.java                # Root aggregate entity for scheduled jobs
│   │   │   │   │   ├── JobExecution.java       # Immutable execution log record with fencing token
│   │   │   │   │   ├── JobStatus.java          # ACTIVE, PAUSED, COMPLETED, CANCELLED
│   │   │   │   │   ├── JobType.java            # ROSTER_GENERATION, PICKUP_ALERT, NO_SHOW_CANCEL, etc.
│   │   │   │   │   ├── MisfirePolicy.java      # FIRE_NOW, SKIP, RESCHEDULE_NEXT
│   │   │   │   │   ├── OutboxEvent.java        # Transactional outbox table entity for Kafka delivery
│   │   │   │   │   ├── PartitionLease.java     # 64-partition lease table entity with fencing tokens
│   │   │   │   │   ├── Priority.java           # CRITICAL, HIGH, MEDIUM, LOW
│   │   │   │   │   ├── ScheduleType.java       # ONE_OFF vs RECURRING_CRON
│   │   │   │   │   ├── Tenant.java             # Multi-tenant boundary entity
│   │   │   │   │   └── User.java               # Auth entity with roles (ADMIN, OPERATOR, TENANT_ADMIN)
│   │   │   │   └── service/                    # Deterministic Algorithmic Components
│   │   │   │       ├── CronResolver.java       # Timezone & DST-aware cron expression evaluator
│   │   │   │       ├── PartitionAssigner.java  # Deterministic hash(jobId) % 64 partition assignment
│   │   │   │       └── RetryCalculator.java    # Exponential backoff with full jitter and capping
│   │   │   │
│   │   │   ├── engine/                         # Core Distributed Scheduling Engine
│   │   │   │   ├── SchedulerEngine.java        # Main poll loop; claims jobs via DB clock index scan
│   │   │   │   ├── LeaseManager.java           # Acquires/renews 64 leases using SKIP LOCKED & DB clock
│   │   │   │   ├── PartitionRebalancer.java    # Balances partitions; sheds leases when over fair-share
│   │   │   │   ├── FencedWriter.java           # CAS completion writer checking monotonic fencing tokens
│   │   │   │   ├── FairnessQueue.java          # In-memory per-tenant semaphores preventing starvation
│   │   │   │   ├── MisfireScanner.java         # Scans overdue jobs on failover and applies policies
│   │   │   │   ├── NodeLifecycle.java          # Node registration, heartbeats, and graceful release
│   │   │   │   └── OutboxPublisher.java        # Poller delivering outbox events to Kafka topics
│   │   │   │
│   │   │   ├── handler/                        # Pluggable Business Job Handlers
│   │   │   │   ├── JobHandler.java             # Execution contract interface execute(JobExecution)
│   │   │   │   ├── JobHandlerRegistry.java     # Registry mapping JobType to concrete handler beans
│   │   │   │   ├── BillingCloseHandler.java    # Handler for end-of-day billing reconciliation
│   │   │   │   ├── CleanupHandler.java         # Handler for historical log and execution cleanup
│   │   │   │   ├── HealthCheckHandler.java     # Periodic health monitoring job handler
│   │   │   │   ├── NoShowCancelHandler.java    # Handler to auto-cancel trips when employee is no-show
│   │   │   │   ├── PickupAlertHandler.java     # Time-critical cab pickup notification handler
│   │   │   │   └── RosterGenerationHandler.java# Roster generation batch job handler
│   │   │   │
│   │   │   ├── repository/                     # Spring Data Repositories & Pessimistic Locks
│   │   │   │   ├── JobRepository.java          # Poll query (FOR UPDATE SKIP LOCKED), cancel query
│   │   │   │   ├── JobExecutionRepository.java # Execution queries and CAS fencing update queries
│   │   │   │   ├── PartitionLeaseRepository.java# Lease acquisition, renewal, and fencing increment
│   │   │   │   ├── OutboxRepository.java       # Outbox table poll and publish status update
│   │   │   │   └── TenantRepository.java       # Tenant lookup queries
│   │   │   │
│   │   │   ├── service/                        # High-Level Business Services
│   │   │   │   ├── JobService.java             # Idempotent job creation, cancel-vs-fire race handling
│   │   │   │   └── TenantService.java          # Tenant pause/resume with Caffeine local caching
│   │   │   │
│   │   │   ├── security/                       # Security & Multi-Tenant Isolation
│   │   │   │   ├── JwtTokenProvider.java       # JWT token parser and validator
│   │   │   │   ├── TenantIsolationFilter.java  # HTTP filter preventing cross-tenant data access
│   │   │   │   └── SeedTokenPrinter.java       # Console printer for pre-signed demo JWT tokens
│   │   │   │
│   │   │   ├── metrics/                        # Observability & Micrometer Instruments
│   │   │   │   └── SchedulerMetrics.java       # Fire lag histograms, lease gauges, failure counters
│   │   │   │
│   │   │   └── exception/                      # Error Handling Layer
│   │   │       ├── GlobalExceptionHandler.java # Maps exceptions to HTTP 4xx/5xx structured JSON
│   │   │       ├── JobNotFoundException.java   # 404 Not Found exception
│   │   │       ├── NonRetryableException.java  # Fast-fail exception skipping backoff retry
│   │   │       ├── SchedulerException.java     # Base unchecked scheduler domain exception
│   │   │       ├── StaleLeaseException.java     # Fencing rejection exception
│   │   │       └── TenantAccessDeniedException.java # 403 Forbidden cross-tenant violation
│   │   │
│   │   └── resources/
│   │       ├── application.yml                 # Primary application properties & Hikari pool settings
│   │       └── db/migration/                   # Flyway Database Migrations
│   │           ├── V1__initial_schema.sql      # Core DDL: tenants, jobs, executions, leases, outbox
│   │           ├── V2__node_heartbeats.sql     # Node heartbeat registry table for cluster discovery
│   │           └── V3__widen_job_execution_status.sql # Widens status enum to accommodate FENCING_REJECTED
│   │
│   └── test/java/com/movesync/scheduler/       # Comprehensive Test Suite (Unit, Integration, Chaos)
│       ├── SchedulerApplicationTests.java     # Spring context bootstrap verification
│       ├── engine/
│       │   ├── CancelRaceTest.java             # G9: Tests cancel-vs-fire race conditions
│       │   ├── FairnessQueueTest.java          # G5: Multi-tenant semaphore fairness tests
│       │   ├── FencingTokenTest.java           # G4: Monotonic fencing CAS stale write rejection
│       │   ├── MisfireHandlerTest.java         # Misfire policy tests (FIRE_NOW, SKIP, RESCHEDULE)
│       │   ├── NodeCrashRecoveryTest.java      # G1: Simulated node kill and failover test
│       │   ├── NodeFreezeTest.java             # Simulates 35s SIGSTOP/GC pause and lease eviction
│       │   └── PartitionCoverageTest.java      # G6: Validates all 64 partitions claimed without gaps
│       ├── exception/
│       │   └── GlobalExceptionHandlerTest.java # Verifies RFC 7807 error responses
│       ├── perf/
│       │   └── SchedulerBenchmark.java         # 100K job ingestion and p50/p95/p99 lag benchmark
│       ├── security/
│       │   └── JwtAuthTest.java                # JWT validation and tenant isolation filter tests
│       └── service/
│           ├── PauseResumeTest.java            # Tenant pause/resume lifecycle tests
│           └── TenantCacheTest.java            # Caffeine cache eviction and refresh tests
│
├── .gitattributes                              # Git line ending normalization
├── .gitignore                                  # Git exclusion list
├── Case Study 3 - Distributed...pdf            # MoveInSync problem statement and specifications
├── commit_plan.md                              # Phased 20-commit engineering roadmap
├── DESIGN_NOTE.md                              # Architectural justifications (partitioning, leases, outbox)
├── docker-compose.yml                          # 3-node scheduler cluster, PostgreSQL 16, Kafka KRaft
├── Dockerfile                                  # Multi-stage container build for Scheduler nodes
├── EXPLANATION.md                              # Deep-dive theory: failure modes, Two Generals, Big-O tables
├── HELP.md                                     # Spring Boot reference documentation
├── mvnw / mvnw.cmd                             # Maven wrapper execution scripts (Unix / Windows)
├── pom.xml                                     # Root Maven project descriptor
├── README.md                                   # This repository overview and navigation guide
├── run_100k_chaos_benchmark.ps1                # Automated 10-minute live chaos benchmark script
└── TRADE_OFFS.md                               # Intentional engineering trade-offs and design constraints
```

---

## Guarantees-to-Code Traceability Matrix

Every single distributed guarantee is implemented in a specific subsystem and verified by a dedicated test suite:

| Guarantee | Description | Key Implementation Files | Verifying Test Suite |
|---|---|---|---|
| **G1: Zero Job Loss** | No job is silently dropped during node crashes or failovers. | [SchedulerEngine.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/SchedulerEngine.java)<br>[MisfireScanner.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/MisfireScanner.java) | [NodeCrashRecoveryTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/NodeCrashRecoveryTest.java) |
| **G2: No Early Firing** | Jobs are never dispatched before `scheduledFireTime`. | [JobRepository.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/repository/JobRepository.java) (`next_fire_time <= NOW()`)<br>[SchedulerEngine.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/SchedulerEngine.java) | [SchedulerEngine.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/SchedulerEngine.java) |
| **G3: Idempotent Creation** | Duplicate client calls return existing job; unique key `{jobId}:{scheduledTime}`. | [JobService.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/service/JobService.java)<br>[JobExecutionRepository.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/repository/JobExecutionRepository.java) | [SchedulerBenchmark.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/perf/SchedulerBenchmark.java) |
| **G4: Zombie Fencing CAS** | Stale nodes waking from long GC pauses cannot overwrite state. | [FencedWriter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FencedWriter.java)<br>[PartitionLeaseRepository.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/repository/PartitionLeaseRepository.java) | [FencingTokenTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/FencingTokenTest.java)<br>[NodeFreezeTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/NodeFreezeTest.java) |
| **G5: Multi-Tenant Fairness** | No single tenant can starve the cluster worker threads. | [FairnessQueue.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FairnessQueue.java) | [FairnessQueueTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/FairnessQueueTest.java) |
| **G6: Full Coverage** | All 64 partitions are always owned across healthy cluster nodes. | [LeaseManager.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/LeaseManager.java)<br>[PartitionRebalancer.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/PartitionRebalancer.java) | [PartitionCoverageTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/PartitionCoverageTest.java) |
| **G7: Transactional Outbox** | State changes and event notifications are committed in a single atomic DB transaction. | [FencedWriter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FencedWriter.java)<br>[OutboxPublisher.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/OutboxPublisher.java) | Unit & Integration test runs |
| **G8: DB Clock Leases** | Leases evaluated strictly via PostgreSQL `NOW()` (immune to JVM clock skew). | [PartitionLeaseRepository.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/repository/PartitionLeaseRepository.java)<br>[LeaseManager.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/LeaseManager.java) | Integrated in LeaseManager queries |
| **G9: Cancel-vs-Fire Race** | Cancellation and dispatch are serialized through row-level locking. | [JobService.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/service/JobService.java) (`SELECT ... FOR UPDATE`) | [CancelRaceTest.java](file:///c:/Users/Asus/Desktop/scheduler/src/test/java/com/movesync/scheduler/engine/CancelRaceTest.java) |

---

## Codebase Navigation: "Where Do I Look When..."

Use this quick navigation guide to quickly find the right files when working on or debugging any part of the system:

### 1. Adding a New Job Type
1. Add the enum constant in [JobType.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/domain/model/JobType.java).
2. Implement [JobHandler.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/handler/JobHandler.java) (e.g., `src/main/java/com/movesync/scheduler/handler/NewJobHandler.java`).
3. Register the new handler in [JobHandlerRegistry.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/handler/JobHandlerRegistry.java).

### 2. Modifying Database Schema & Queries
1. Add a new migration script in [src/main/resources/db/migration/](file:///c:/Users/Asus/Desktop/scheduler/src/main/resources/db/migration/) (e.g., `V4__add_custom_index.sql`).
2. Update the corresponding JPA entity in [domain/model/](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/domain/model/).
3. Adjust queries or locking in [repository/](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/repository/).

### 3. Tuning Concurrency, Worker Pools & Poll Intervals
1. Default pool sizes and poll timing are in [application.yml](file:///c:/Users/Asus/Desktop/scheduler/src/main/resources/application.yml) under the `scheduler:` block.
2. Container-specific overrides for cluster testing are in [docker-compose.yml](file:///c:/Users/Asus/Desktop/scheduler/docker-compose.yml).
3. Thread pool definitions (Core vs Critical pool) are created in [BeanConfig.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/config/BeanConfig.java).
4. Type-safe configuration binding is located in [SchedulerProperties.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/config/SchedulerProperties.java).

### 4. Adjusting Multi-Tenant Fairness & Quotas
1. Concurrency limits per tenant are managed by [FairnessQueue.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FairnessQueue.java).
2. Tenant pause/resume and caching are managed in [TenantService.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/service/TenantService.java).
3. Tenant boundary security checks are handled in [TenantIsolationFilter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/security/TenantIsolationFilter.java).

### 5. Inspecting or Replaying Dead-Lettered Executions
1. Dead-letter API endpoints are defined in [DeadLetterController.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/api/DeadLetterController.java).
2. Terminal execution status logic is handled in [FencedWriter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FencedWriter.java).
3. Retry limits and backoff calculations are in [RetryCalculator.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/domain/service/RetryCalculator.java).

### 6. Working with the Kafka Outbox Publisher
1. Outbox table polling and publishing loop is in [OutboxPublisher.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/OutboxPublisher.java).
2. Kafka producer options and idempotence are in [KafkaProducerConfig.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/config/KafkaProducerConfig.java).
3. Events are atomically inserted alongside execution status updates in [FencedWriter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/engine/FencedWriter.java).

### 7. Running Tests & Verifying Guarantees
- Run all unit and integration tests:
  ```powershell
  .\mvnw.cmd test
  ```
- Run a specific test suite:
  ```powershell
  .\mvnw.cmd test -Dtest=CancelRaceTest
  .\mvnw.cmd test -Dtest=FencingTokenTest
  .\mvnw.cmd test -Dtest=NodeCrashRecoveryTest
  ```
- Run the 100K Chaos Benchmark with live container killing:
  ```powershell
  .\run_100k_chaos_benchmark.ps1
  ```

---

## Interactive API & Authentication

Interactive Swagger documentation is exposed on all running nodes:

- **Swagger UI:** `http://localhost:8080/swagger-ui.html`
- **OpenAPI JSON Docs:** `http://localhost:8080/v3/api-docs`
- **Prometheus Metrics:** `http://localhost:8080/actuator/prometheus`
- **Health Check:** `http://localhost:8080/actuator/health`

### Authentication (Tenant-Scoped RBAC)

On startup, [SeedTokenPrinter.java](file:///c:/Users/Asus/Desktop/scheduler/src/main/java/com/movesync/scheduler/security/SeedTokenPrinter.java) outputs pre-signed JWT Bearer tokens to the logs for easy copy-pasting into Swagger UI or cURL:

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

## API Endpoints Reference

| Method | Path | Description | Access / RBAC |
|---|---|---|---|
| POST | `/api/tenants/{tenantId}/jobs` | Create a new scheduled job | Tenant Admin / Admin |
| GET | `/api/tenants/{tenantId}/jobs` | List tenant's active/completed jobs | Tenant Admin / Admin |
| GET | `/api/tenants/{tenantId}/jobs/{jobId}` | Get details of a specific job | Tenant Admin / Admin |
| POST | `/api/tenants/{tenantId}/jobs/{jobId}/cancel` | Cancel an active job safely | Tenant Admin / Admin |
| POST | `/api/tenants/{tenantId}/jobs/{jobId}/trigger` | Manually trigger job execution immediately | Tenant Admin / Admin |
| GET | `/api/tenants/{tenantId}/jobs/{jobId}/next-fire-times` | Preview the next $N$ scheduled fire times | Tenant Admin / Admin |
| POST | `/api/tenants/{tenantId}/pause` | Pause all scheduled executions for tenant | Tenant Admin / Admin |
| POST | `/api/tenants/{tenantId}/resume` | Resume scheduled executions for tenant | Tenant Admin / Admin |
| GET | `/api/tenants/{tenantId}/dead-letter` | List dead-lettered job executions | Operator / Admin |
| POST | `/api/tenants/{tenantId}/dead-letter/{execId}/replay` | Replay a failed job execution | Operator / Admin |
| GET | `/actuator/health` | Node health check and DB/Kafka status | Public |
| GET | `/actuator/prometheus` | Micrometer metrics for Prometheus scraping | Public |
| GET | `/swagger-ui.html` | Interactive OpenAPI documentation UI | Public |

---

## How to Run

### Option 1: 3-Node Cluster via Docker Compose (Recommended)

Spins up PostgreSQL 16 (configured for 300 connections), Kafka 3.8 (KRaft mode), and 3 scheduler nodes:

```powershell
# Start PostgreSQL, Kafka, and 3 scheduler nodes
docker compose up --build -d

# Follow logs of Node 1 (contains startup JWT seed tokens)
docker compose logs -f scheduler-node1

# Verify all 3 nodes have claimed their share of the 64 partition leases
docker compose exec postgres psql -U scheduler -d scheduler_db -c "SELECT node_id, count(*) FROM partition_leases GROUP BY node_id ORDER BY node_id;"
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

## Memory & Resource Sizing

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

- [Demo Walkthrough & Failover Video](https://drive.google.com/drive/folders/1EuAMylkT7YqvRZ7Ju1IkQQkO2z2Y0kBC) — Complete video walkthrough demonstrating job execution, tenant isolation, and live node kill failover.
- [Detailed Architecture & Fencing Explanation (EXPLANATION.md)](file:///c:/Users/Asus/Desktop/scheduler/EXPLANATION.md) — Exhaustive deep-dive covering failure modes, zombie fencing diagrams, Two Generals Problem, cancel race, and Big-O tables.
- [Design Decisions Note (DESIGN_NOTE.md)](file:///c:/Users/Asus/Desktop/scheduler/DESIGN_NOTE.md) — Detailed rationale for `hash(jobId)` vs `hash(tenantId)`, DB-backed lease design, and transactional outbox pattern.
- [Engineering Trade-Offs (TRADE_OFFS.md)](file:///c:/Users/Asus/Desktop/scheduler/TRADE_OFFS.md) — Explicit documentation of intentional architectural trade-offs and operational boundary limits.
- [Phased Commit Plan (commit_plan.md)](file:///c:/Users/Asus/Desktop/scheduler/commit_plan.md) — 20-commit phased engineering plan detailing each step of development.
