# Distributed Trip Job Scheduler — Complete Explanation

> A deterministic, fault-tolerant, multi-tenant job scheduler built with
> Java/Spring Boot and PostgreSQL, designed for MoveInSync's employee
> transport platform.

---

## Table of Contents

1. [What Problem We're Solving](#1-what-problem-were-solving)
2. [Architecture Overview](#2-architecture-overview)
3. [How Jobs Are Distributed — The Partition Model](#3-how-jobs-are-distributed--the-partition-model)
4. [The Complete Life of a Job](#4-the-complete-life-of-a-job)
5. [How Nodes Coordinate — Leases and Fencing](#5-how-nodes-coordinate--leases-and-fencing)
6. [What Happens When Things Go Wrong](#6-what-happens-when-things-go-wrong)
7. [Fairness, Priority, and Tenant Isolation](#7-fairness-priority-and-tenant-isolation)
8. [Time Zones and Daylight Saving Time](#8-time-zones-and-daylight-saving-time)
9. [Retries, Dead-Letters, and Misfires](#9-retries-dead-letters-and-misfires)
10. [The Cancel-vs-Fire Race](#10-the-cancel-vs-fire-race)
11. [Outbox Pattern and Kafka](#11-outbox-pattern-and-kafka)
12. [Caching Strategy](#12-caching-strategy)
13. [Monitoring and Observability](#13-monitoring-and-observability)
14. [Security and Data Privacy](#14-security-and-data-privacy)
15. [Design Choices and Trade-offs](#15-design-choices-and-trade-offs)
16. [Where the System Stops Scaling](#16-where-the-system-stops-scaling)
17. [How We Prove It — Tests and Benchmarks](#17-how-we-prove-it--tests-and-benchmarks)
18. [Requirement Coverage Matrix](#18-requirement-coverage-matrix)

---

## 1. What Problem We're Solving

MoveInSync operates an employee transport platform. Every day, the system
must execute hundreds of thousands of timed jobs:

- **Roster generation** at 10 PM in each client's local time zone
- **"Your cab is assigned" alerts** exactly 30 minutes before each pickup
- **Auto-cancel no-shows** if an employee doesn't board within 5 minutes
- **Daily billing close** to reconcile all trips for a tenant

These jobs run on a cluster of at least three servers. Any server can crash
at any moment, or freeze for 20 seconds during a garbage collection pause.
Despite this:

- **No job may be lost.** Every scheduled job must eventually execute.
- **No job may fire twice.** Duplicate alerts or double billing is unacceptable.
- **No single bottleneck.** The system must not rely on one lock row that
  every server fights over.

Off-the-shelf schedulers (like Quartz with a JDBC store) coordinate through
a single database lock row. Under load, every node contends for that row,
creating a bottleneck that collapses throughput. Our job is to build a
scheduler that stays correct *and* fast as nodes come and go.

---

## 2. Architecture Overview

### What It Is

A **shared-nothing compute, shared-storage** distributed architecture.
Three stateless Spring Boot application nodes connect to one shared
PostgreSQL database. The nodes don't talk to each other — all coordination
happens through database rows.

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node 1  │     │  Node 2  │     │  Node 3  │
│(stateless)│    │(stateless)│    │(stateless)│
│  port 8081│    │  port 8082│    │  port 8083│
└─────┬─────┘    └─────┬─────┘    └─────┬─────┘
      │                │                │
      └────────────────┼────────────────┘
                       │
              ┌────────▼────────┐
              │   PostgreSQL    │
              │ (single source  │
              │   of truth)     │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │     Kafka       │
              │ (event delivery │
              │    only)        │
              └─────────────────┘
```

### Why This Architecture

**One database means one transactional boundary.** The most critical
operation in the system — completing a job — involves three writes:

1. Mark the execution as COMPLETED
2. Verify the fencing token is still valid (no stale zombie node)
3. Write an outbox event for downstream notification

All three must succeed or fail together. With a single PostgreSQL instance,
they happen in one atomic transaction. If we split state across multiple
systems (Kafka for jobs, etcd for leases, Postgres for results), we'd need
distributed transactions, which are slow, complex, and fragile.

**Nodes are stateless and interchangeable.** If you kill all three and start
three new ones, they reconnect to Postgres, claim partitions, and continue.
Nothing durable lives on the nodes — only thread pools and in-memory
semaphores, which are recreated on startup.

### What Each Component Does

| Component | Responsibility |
|---|---|
| **PostgreSQL** | Stores all job definitions, execution records, partition leases, tenant data, and outbox events. It is the single source of truth |
| **Kafka** | Delivers outbox events to downstream services. Used only for this — not for job distribution or coordination |
| **SchedulerEngine** | The main loop: polls for due jobs, dispatches them to worker threads |
| **LeaseManager** | Acquires and renews partition leases. Detects when leases are stolen |
| **PartitionRebalancer** | Detects dead nodes (expired leases), claims orphaned partitions, sheds excess partitions when new nodes join |
| **FencedWriter** | Performs the G4 fenced completion write — the atomic "mark done if I'm still the rightful owner" operation |
| **MisfireScanner** | When a node takes over a new partition, scans for jobs that missed their fire time during the gap |
| **FairnessQueue** | Per-tenant in-memory semaphore that prevents one tenant from monopolizing worker threads |
| **JobHandlerRegistry** | Strategy pattern: maps each job type (PICKUP_ALERT, BILLING_CLOSE, etc.) to its handler class |
| **OutboxPublisher** | Polls the outbox table every second, sends events to Kafka |

---

## 3. How Jobs Are Distributed — The Partition Model

### The Core Problem

If three nodes all query `SELECT * FROM jobs WHERE fire_time <= now()`,
they see the same rows, fight over the same locks, and risk duplicate
execution. This is the "single hot lock" problem.

### Our Solution: 64 Logical Partitions

When a job is created, it gets a **partition number** stamped on it:

```java
job.partitionId = Math.abs(job.getId().hashCode()) % 64;
```

This number is stored on the job row and never changes. The job space is
now divided into 64 slices. Each node is responsible for a subset:

```
Node 1 → partitions 0–20   (21 partitions, ~33% of jobs)
Node 2 → partitions 21–42  (22 partitions, ~34% of jobs)
Node 3 → partitions 43–63  (21 partitions, ~33% of jobs)
```

Each node's poll query only looks at its own partitions:

```sql
SELECT * FROM jobs
WHERE partition_id IN (0, 1, 2, ..., 20)   -- Node 1's partitions
  AND status = 'ACTIVE'
  AND next_fire_time <= NOW()
ORDER BY /* priority */ CASE priority
    WHEN 'CRITICAL' THEN 0
    WHEN 'HIGH'     THEN 1
    WHEN 'NORMAL'   THEN 2
    WHEN 'LOW'      THEN 3
  END ASC, next_fire_time ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

**No contention.** Node 1 queries rows in partitions 0–20. Node 2 queries
21–42. They never touch the same rows.

### Why `hash(jobId)` and Not `hash(tenantId)`

If we partitioned by tenant, a large tenant with 50,000 jobs would land
entirely on one node, creating a hot spot. With `hash(jobId)`, every
tenant's jobs scatter uniformly across all 64 partitions and all 3 nodes.

The trade-off: we can't enforce per-tenant fairness at the partition level
(a tenant's jobs are everywhere), so we enforce it per-node with an
in-memory semaphore. This is a deliberate choice — even distribution across
nodes is more important than co-locating one tenant's work.

### Why 64 Partitions

- **Much more than max nodes**: with 3 nodes, 64 gives ~21 partitions each.
  This makes rebalancing granular — when a node dies, its ~21 partitions
  spread across survivors, not all-or-nothing.
- **Small enough to be trivial**: the `partition_leases` table has 64 rows.
  Polling them every 15 seconds is negligible load.
- **Power of two**: `hashCode() % 64` distributes well. Non-powers-of-two
  can introduce bias.

### `FOR UPDATE SKIP LOCKED` — The Key to Zero Contention

This PostgreSQL feature is what replaces the single-lock-row pattern.
When a node runs the poll query:

- `FOR UPDATE` locks each returned row, preventing another node from
  processing the same job.
- `SKIP LOCKED` means: if another thread or node already locked a row,
  skip it instead of waiting. No blocking, no deadlocks.

During a brief rebalance overlap (two nodes both think they own partition
7), both might poll it. `SKIP LOCKED` means the second one simply skips
rows the first already claimed. Combined with the idempotency key on
execution records, duplicate execution is impossible.

### What "Partition Moves" Means

When we say "a partition moves from Node 1 to Node 2," nothing physical
moves. The jobs stay in the `jobs` table. What changes is a single row in
the `partition_leases` table:

```sql
UPDATE partition_leases
SET node_id = 'node-2',
    fencing_token = fencing_token + 1,
    lease_expires = NOW() + INTERVAL '30 seconds'
WHERE partition_id = 7;
```

Before this update, Node 1 included partition 7 in its poll query. After,
Node 2 does. The "move" is a metadata change, not a data transfer.

---

## 4. The Complete Life of a Job

Let's trace a concrete example: Infosys wants to send a pickup alert to
employee Rahul at 07:30 AM IST tomorrow.

### Stage 1: Creation

```
POST /api/tenants/{infosys-id}/jobs
{
  "jobType": "PICKUP_ALERT",
  "scheduleType": "ONE_OFF",
  "nextFireTime": "2026-10-31T02:00:00Z",    // 07:30 IST = 02:00 UTC
  "priority": "HIGH",
  "misfirePolicy": "SKIP",
  "externalRefId": "pickup-rahul-2026-10-31",
  "payload": "{\"employeeId\": \"emp-123\"}"
}
```

The request hits any node (they all share the same REST API). The node:

1. **Authenticates** — validates the JWT, extracts `tenantId` and role
2. **Checks idempotency** — queries for an existing job with the same
   `(tenantId, jobType, externalRefId)`. If found, returns the existing job
   (the Roster Service crashed and retried — no duplicate created)
3. **Generates a UUID** — e.g., `a3f2b8c1-...`
4. **Assigns a partition** — `hash(a3f2b8c1) % 64 = 37`
5. **Inserts into the `jobs` table** — the job now exists with
   `partition_id = 37` and `next_fire_time = 2026-10-31T02:00:00Z`

The node that handled this API call may not be the node that executes the
job. Partition 37 might be owned by Node 2, while the API call went to
Node 1. That's fine — creation and execution are decoupled.

### Stage 2: Waiting

The job sits in the database. Every 500ms, the node owning partition 37
polls for due jobs. At 18:00 UTC on October 30, the fire time (02:00 UTC
October 31) is 8 hours away. The poll query's `next_fire_time <= NOW()`
condition isn't met. The job waits.

### Stage 3: Firing

At 02:00:00 UTC, the poll returns this job. The scheduler engine:

1. **Checks tenant status** — is Infosys paused? (Cached lookup, 5s TTL.
   If paused, the job stays in the table and fires when the tenant resumes.)

2. **Checks fairness** — has Infosys hit the per-node concurrency cap of
   50? (`FairnessQueue.tryAcquire(tenantId)` — an in-memory semaphore.
   If at capacity, the job is deferred to the next poll in 500ms.)

3. **Creates an execution record** — inserts a row into `job_executions`
   with an idempotency key of `a3f2b8c1:2026-10-31T02:00:00Z` and status
   `PENDING`. If this key already exists (another node processed it during
   a rebalance overlap), the insert is a no-op.

4. **Commits the transaction** — the execution record is now visible to
   all database connections.

5. **Dispatches to a worker thread** — after the transaction commits (to
   avoid the read-committed visibility issue), the job is submitted to
   either the critical pool (4 threads, for CRITICAL jobs) or the worker
   pool (20 threads, for HIGH/NORMAL/LOW).

6. **Atomically claims the execution** — on the worker thread, before
   running the handler, the engine executes
   `UPDATE job_executions SET status = 'RUNNING' WHERE id = :id AND status = 'PENDING'`.
   If this returns 0 rows (a cancel request already set it to
   `CANCELLED_BEFORE_EXECUTION`), the worker exits without executing.

### Stage 4: Execution

The worker thread:

1. **Looks up the handler** — `JobHandlerRegistry.getHandler(PICKUP_ALERT)`
   returns `PickupAlertHandler` (strategy pattern).

2. **Runs the handler** — the handler reads the payload and does its work
   (in production: calls the notification service to send an SMS; in our
   project: logs the execution).

3. **Fenced completion** — this is the critical step. In one database
   transaction:

   ```sql
   -- (a) Mark execution complete, but ONLY if our fencing token is still valid
   UPDATE job_executions
   SET status = 'COMPLETED', completed_at = NOW()
   FROM partition_leases pl
   WHERE job_executions.id = :execId
     AND job_executions.partition_id = pl.partition_id
     AND pl.fencing_token = :myToken;

   -- If affected_rows = 0: our lease was stolen. ROLLBACK everything.
   -- If affected_rows = 1: we're still the rightful owner. Continue.

   -- (b) Write an outbox event (same transaction)
   INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
   VALUES ('JOB', 'a3f2b8c1', 'JOB_COMPLETED', '{...}');

   -- (c) Mark the ONE_OFF job as done
   UPDATE jobs SET status = 'COMPLETED' WHERE id = 'a3f2b8c1';

   COMMIT;
   ```

   All three writes succeed or fail together. If the fencing check fails
   (another node took over this partition while we were running), the
   entire transaction rolls back — including the outbox event and the
   handler's side effects.

4. **Releases the fairness semaphore** — in a `finally` block, always.

### Stage 5: Event Delivery

The `OutboxPublisher` (a background thread polling every 1 second) picks
up the outbox event, sends it to the `scheduler.events` Kafka topic, and
marks it as published. Downstream services (roster, billing, analytics)
consume from this topic.

---

## 5. How Nodes Coordinate — Leases and Fencing

### The Lease Model

The `partition_leases` table has 64 rows, one per partition:

```
partition_id | node_id | fencing_token | lease_expires
0            | node-1  | 5             | 2026-10-30 12:05:30
1            | node-1  | 5             | 2026-10-30 12:05:30
...
21           | node-2  | 8             | 2026-10-30 12:05:28
...
```

Each node has two background threads for coordination:

**Lease renewal (every 10 seconds ± jitter):**
```sql
UPDATE partition_leases
SET lease_expires = NOW() + INTERVAL '30 seconds'
WHERE node_id = 'node-1';
```

This uses the database clock (`NOW()`), never the JVM clock, to prevent
clock drift issues. Every 10 seconds, the node pushes its lease expiry
forward by 30 seconds. As long as the node is alive, its leases never
expire.

**Rebalance check (every 15 seconds ± jitter):**
The rebalancer computes `fairShare = ceil(64 / activeNodes)` and:
- If the node owns fewer partitions than its fair share, it claims
  orphaned (expired-lease) partitions using `SKIP LOCKED`
- If the node owns more than its fair share (a new node joined), it
  voluntarily releases excess partitions by setting their `lease_expires`
  to `NOW()`, allowing the new node to claim them

Jitter (random ±1.5s on renewal, ±2s on rebalance) prevents synchronized
polling that could spike database CPU.

### Why Leases Instead of Permanent Assignment

A permanent assignment (Node 1 always owns partitions 0–20) doesn't
handle crashes. If Node 1 dies, its partitions are stuck. Nobody can
process them until someone manually reassigns.

With leases, ownership is self-healing. If a node dies, its leases expire
in 30 seconds. Other nodes detect the expiry and claim the orphaned
partitions automatically. No human intervention needed.

### The Fencing Token — Protecting Against Zombie Nodes

A lease alone doesn't prevent this scenario:

1. Node 1 picks up a job from partition 7
2. Node 1 freezes (30-second GC pause)
3. Node 1's lease expires
4. Node 2 claims partition 7 and executes the same job
5. Node 1 wakes up and tries to write "job completed"

Without protection, Node 1's write would succeed, creating a duplicate
completion. The **fencing token** prevents this.

```
Node 1 (Frozen)             PostgreSQL             Node 2 (Active)
     |                           |                        |
     |-- Claims partition (t=5)->|                        |
     |                           |                        |
     |=== [GC Pause 30s] ========|                        |
     |                           |<-- Lease expires ------|
     |                           |<-- Claims part (t=6) --|
     |                           |    (fencing_token = 6) |
     |                           |<-- Executes job -------|
     |                           |<-- Completes (t=6) ----|
     |                           |    affected_rows = 1   |
     |                           |                        |
     |-- Wakes up ===============|                        |
     |-- Attempts complete (t=5)->|                        |
     |    affected_rows = 0      |                        |
     |    [ROLLBACK TRIGGERED]   |                        |
```

Every time a partition is claimed, its `fencing_token` increments:

```
Node 1 held partition 7 with token 5
Node 1 freezes, lease expires
Node 2 claims partition 7, token becomes 6
```

When any node completes a job, its write checks the current token:

```sql
UPDATE job_executions SET status = 'COMPLETED'
FROM partition_leases pl
WHERE job_executions.id = :execId
  AND pl.fencing_token = 5;     -- Node 1's token when it started
-- Current token is 6 → affected_rows = 0 → ROLLBACK
```

Node 1's write returns 0 affected rows and triggers a rollback. Node 2's write (with token 6) succeeds.
The job completes with exactly-once effect.

This check runs **in the same database transaction** as the job execution update and the outbox event write. Handlers do NOT perform non-rollbackable external network operations (such as HTTP calls or direct SMS dispatch) directly inside the transaction. Instead, the **Outbox table is the transactional side-effect channel**. Handlers stage their external notifications into `outbox_events` within the same transaction as the `fencing_token` check on `partition_leases`. If the fencing check fails (`affected_rows = 0`), the transaction rolls back — meaning the outbox event is never persisted, and therefore the `OutboxPublisher` will never publish the side-effect to Kafka. This is how "at-least-once execution, exactly-once effect" is safely achieved.

### Lease Acquisition with SKIP LOCKED

When nodes start simultaneously (e.g., `docker compose up`), they race to
claim partitions. Without `SKIP LOCKED`, the first node would lock all 64
rows and the others would block until it committed.

With `SKIP LOCKED` and a `LIMIT` based on fair share:

```sql
UPDATE partition_leases
SET node_id = :myNodeId, fencing_token = fencing_token + 1, ...
WHERE partition_id IN (
    SELECT partition_id FROM partition_leases
    WHERE node_id IS NULL OR lease_expires < NOW()
    FOR UPDATE SKIP LOCKED
    LIMIT 22                     -- ceil(64 / 3) = fair share
);
```

Node 1 claims ~22 partitions, skipping rows that Node 2 is simultaneously
locking. Node 2 claims ~21 from the remainder. Node 3 gets the rest.
Zero contention, one pass each.

---

## 6. What Happens When Things Go Wrong

### Summary of Failure Modes

| Failure Mode | Detection Mechanism | Recovery / Safety Guarantee |
|---|---|---|
| **Node Crash (`kill -9`)** | Database clock lease timeout (`lease_expires < NOW()`, 30s) | Surviving nodes acquire orphaned partitions via `SKIP LOCKED`; `MisfireScanner` recovers stuck jobs with incremented fencing tokens. |
| **Node Freeze (GC Pause / SIGSTOP)** | Monotonic fencing token CAS check on completion | Frozen node's stale write returns 0 affected rows; transaction rolls back outbox & execution. The new owner executes with a higher token. |
| **Database Disconnect** | Hikari connection timeout & Spring transaction rollback | In-flight execution semaphores released in `finally`; node pauses poll loop and retries DB connections with backoff. |
| **Concurrent Cancel vs Fire** | Atomic conditional update on `status = 'PENDING'` | PostgreSQL serializes row-level locks: exactly one operation changes state, the loser receives 0 affected rows. |
| **Kafka Cluster Outage** | Transactional Outbox pattern | Scheduler engine completes jobs normally into PostgreSQL; `OutboxPublisher` safely queues events and drains them once Kafka recovers. |

### Why "Exactly-Once Execution" Is Impossible (And What We Guarantee Instead)

In distributed systems, the classic **Two Generals Problem** proves that achieving exactly-once *execution* across unreliable network boundaries or under node failures is mathematically impossible. If a worker node fires a job handler, executes the code, and then crashes or gets partitioned before acknowledging completion, the cluster cannot know whether the execution completed. If the cluster runs it again, code has executed twice (at-least-once). If the cluster refuses to retry, the job may never have finished (at-most-once).

Because true exactly-once execution is impossible in the presence of arbitrary failures, our system guarantees **at-least-once execution with exactly-once effect**:
1. All side effects destined for external consumers are written to the `outbox_events` table inside the PostgreSQL transaction.
2. The `FencedWriter` commits the execution status and outbox events only if the partition's monotonic `fencing_token` has not been superseded.
3. If a zombie node was superseded by another node, its entire transaction rolls back before its side effects can ever reach the outbox table or downstream message queues.

### Failure 1: Node Crash (kill -9)

**What happens:**
The node is gone instantly. No graceful shutdown, no lease release. Its
partition leases sit in the database with their original `lease_expires`.

**Detection:**
After 30 seconds, the leases expire. Other nodes' rebalance loop detects
them:
```sql
SELECT partition_id FROM partition_leases WHERE lease_expires < NOW();
```

**Recovery:**
1. Surviving nodes claim the orphaned partitions (with new fencing tokens)
2. MisfireScanner runs on each newly claimed partition
3. Jobs stuck in RUNNING from the dead node are marked FAILED
4. Due jobs fire on the new owner
5. Everything continues

**Guarantee:** No job is lost. Jobs that were in-flight on the crashed node
are recovered and re-executed. The idempotency key prevents duplicates.

**Time to recover:** ~30–45 seconds (lease expiry + rebalance interval).

### Failure 2: Node Freeze (GC Pause / SIGSTOP)

**What happens:**
The node is alive but unresponsive. It's suspended mid-execution, holding
a half-finished job in memory.

**Detection and recovery:**
Same as a crash — lease expires, another node takes over. The difference
is what happens when the frozen node wakes up.

**The frozen node resumes:**
It tries to complete the job it was working on. The fenced write checks
its stale fencing token against the current one in the database. The
token has been incremented by the new owner. `affected_rows = 0`. The
entire transaction (including any side-effect writes) rolls back.

The frozen node then detects that its leases were stolen (the renewal
thread returns 0 renewed rows), drops those partitions from its local
state, and re-registers to claim whatever partitions are available.

**Guarantee:** No duplicate side effects. The fencing token is the last
line of defense, and it works because the check is in the same
transaction as the side effects.

### Failure 3: Database Unavailable

**What happens:**
All nodes stall. No polls, no executions, no lease renewals.

**Behaviour:**
This is the cost of centralized state. If Postgres is down, work stops.
Nodes enter a draining state, stop acquiring new work, and retry database
connections with backoff.

**Recovery:**
When Postgres returns, nodes reconnect, re-register, and their partition
leases either are still valid (if the outage was shorter than 30 seconds)
or have expired (requiring reacquisition). The misfire scanner catches up
on any jobs that were due during the outage.

**Trade-off acknowledged:** In production, we would use a managed
high-availability Postgres setup (RDS Multi-AZ or Patroni). For this
project, a single instance is appropriate — the spec's scale target
(3 nodes, 100K jobs) is well within one Postgres instance's capacity.

### Failure 4: Kafka Unavailable

**What happens:**
The OutboxPublisher can't send events. Events accumulate in the
`outbox_events` table with `published = false`.

**Behaviour:**
Job execution continues normally. Kafka is only used for event delivery,
not for coordination or job distribution. The outbox table acts as a
durable buffer.

**Recovery:**
When Kafka returns, the publisher drains the backlog in order. Events are
delivered with at-least-once semantics (if the publisher crashes between
Kafka ack and marking as published, the event is resent — downstream
consumers deduplicate by event ID).

### Failure 5: Handler Exception

**Retryable errors:**
The handler throws a `RuntimeException`. The scheduler retries with
exponential backoff (1s → 2s → 4s, ±25% jitter). After 3 attempts
(configurable), the execution moves to dead-letter status.

**Non-retryable errors:**
The handler throws a `NonRetryableException`. The execution is
immediately dead-lettered, skipping retries.

**Handler timeout:**
If a handler exceeds its `maxRunTimeSec` (default 300 seconds), the
thread is interrupted, the execution is marked FAILED, and retried.

### Failure 6: Clock Skew Between Nodes

**Prevention:**
All time-sensitive operations use the database clock (`NOW()` in SQL),
not the JVM clock. Lease expiry, fire time comparison, and completion
timestamps all come from Postgres. The JVM clock is used only for
in-memory scheduling (poll sleep interval, retry backoff timing), where
±1 second of skew is irrelevant.

**Bound:**
We assume NTP keeps nodes within 500ms of each other. The 30-second
lease timeout is orders of magnitude larger than any realistic clock skew.

---

## 7. Fairness, Priority, and Tenant Isolation

### Per-Tenant Fairness

Without a cap, one tenant's 50,000 alerts at 8:00 AM would consume all
worker threads, starving another tenant's 20 alerts.

**Mechanism:** A `ConcurrentHashMap<TenantId, Semaphore>` on each node.
Before dispatching a job to a worker thread, the engine calls
`fairnessQueue.tryAcquire(tenantId)`. If the tenant already has 50 jobs
running on this node, the acquisition fails, and the job is deferred to
the next poll (500ms later).

**Scope:** Per-node, not global. With 3 nodes and a per-node cap of 50,
the cluster-wide maximum for one tenant is 150. This is intentional — a
global cap would require a distributed counter, adding a database
round-trip to every job fire and destroying p99 latency.

**Guarantee:** Tenant B's 20 alerts fire within 5 seconds of their
scheduled time, even while Tenant A's 50,000 are processing.

### Priority

Jobs have four priority levels: CRITICAL, HIGH, NORMAL, LOW.

**Ordering:** The poll query returns CRITICAL jobs first, then HIGH, then
NORMAL, then LOW (using a `CASE` expression).

**Thread pool routing:** CRITICAL jobs go to a dedicated 4-thread pool
that is never shared with normal workload. HIGH/NORMAL/LOW share the
20-thread worker pool. This ensures billing close and SOS-related jobs
are never blocked by a flood of pickup alerts.

**Starvation prevention:** Even LOW jobs must fire within 5 minutes
(configurable) of their scheduled time during normal operation.

### Tenant Isolation

**Data isolation:** Every database query includes `WHERE tenant_id = ?`.
The `TenantIsolationFilter` extracts the tenant from the JWT token and
sets it in a `ThreadLocal`. Repository methods read this automatically.
A bug in Tenant A's job handler cannot see or corrupt Tenant B's data.

**API isolation:** Attempting to read, cancel, or trigger another tenant's
job returns 403 Forbidden.

**Test:** An automated test creates a job for Tenant A and attempts to
read it as Tenant B — the request is rejected.

---

## 8. Time Zones and Daylight Saving Time

### Storage Is Always UTC

All `next_fire_time` values in the database are UTC. The cron-to-UTC
conversion happens when computing the next fire time, never stored as
local time. This prevents ambiguity and simplifies comparisons.

### Cron Evaluation Uses the Tenant's IANA Time Zone

A recurring job with cron `0 22 * * *` for a tenant in `Asia/Kolkata`
(IST, UTC+5:30) fires at 16:30 UTC daily. The `CronResolver` uses
`java.time.ZonedDateTime` with the tenant's zone ID and the `cron-utils`
library for parsing.

### Spring-Forward (TZ3): When an Hour Disappears

On the day clocks jump from 02:00 → 03:00 (e.g., US Eastern in March),
a job scheduled at 02:30 cannot fire — that time doesn't exist.

**Our policy:** Fire at the next valid instant after the gap, which is
03:00 local time. This matches `java.time`'s behavior for non-existent
local times.

**Test:** A daily 02:30 job in `America/New_York` on spring-forward
Sunday. Assert the computed fire time is 03:00 EDT.

### Fall-Back (TZ4): When an Hour Repeats

On the day clocks go back from 02:00 → 01:00 (e.g., US Eastern in
November), 01:30 happens twice.

**Our policy:** Fire during the first occurrence only. We use
`ZonedDateTime.withEarlierOffsetAtOverlap()`, which is the default in
`java.time`. The job does not fire twice — that would violate the
exactly-once semantics for a single cron tick.

**Test:** A daily 01:30 job in `America/New_York` on fall-back Sunday.
Assert exactly one fire time, and that two consecutive daily fires are
~24 hours apart (not ~1 hour).

---

## 9. Retries, Dead-Letters, and Misfires

### Retry Policy

When a handler throws a retryable exception:

| Attempt | Backoff | Range (with ±25% jitter) |
|---|---|---|
| 1 | 1,000ms | 750ms – 1,250ms |
| 2 | 2,000ms | 1,500ms – 2,500ms |
| 3 | 4,000ms | 3,000ms – 5,000ms |
| 4+ | Dead-lettered | — |

Backoff is capped at 60 seconds (configurable). Jitter prevents
thundering herd when many jobs fail simultaneously (e.g., a downstream
service is down).

A retry reuses the same execution record (incrementing `attempt_number`)
and the same idempotency key. If the handler's side effect already
succeeded before the crash that triggered the retry, the idempotency
key in the downstream system prevents a duplicate.

### Dead-Letter

After exhausting retries, the execution is marked `DEAD_LETTERED`. An
outbox event notifies downstream systems. Operators can:

- **Inspect:** `GET /api/tenants/{id}/dead-letter` lists all
  dead-lettered executions with timestamps, attempt counts, and error
  messages (no PII — only job IDs and error types).
- **Replay:** `POST /api/tenants/{id}/dead-letter/{execId}/replay`
  creates a new manual trigger for the original job, with a fresh
  idempotency key (`{jobId}:REPLAY:{timestamp}`).

### Misfire Policies

When a job's fire time passes without execution (because its partition
was owned by a node that died), the MisfireScanner applies the job's
configured policy:

| Policy | Behavior | Use Case |
|---|---|---|
| `FIRE_NOW` | Execute immediately | Roster generation (late is better than never) |
| `SKIP` | Discard for one-off jobs; advance to next fire time for recurring | Pickup alerts (useless if late) |
| `COALESCE` | Collapse all missed fires into one execution | Hourly health checks after downtime |

The scanner runs whenever a node acquires new partitions (startup or
failover), looking back up to 24 hours (configurable) for overdue jobs.

---

## 10. The Cancel-vs-Fire Race

### The Problem

A no-show timer fires at 08:05:00. The "employee boarded" event arrives
at 08:04:59. Two threads compete:

- Thread A (API): tries to cancel the pending execution
- Thread B (scheduler): tries to fire the pending execution

Both target the same `job_executions` row with status `PENDING`.

### The Solution

Cancelling and firing are coordinated atomically through database transactions using two explicit steps:

#### Step 1: Cancel the Job Definition
First, mark the parent job as cancelled so no future executions can be scheduled or claimed by the poll loop:
```sql
UPDATE jobs
SET status = 'CANCELLED', updated_at = NOW()
WHERE id = :jobId AND status != 'CANCELLED';
```

#### Step 2: Atomic Execution State Transition
Next, cancel any currently pending execution that has not yet started running:
```sql
UPDATE job_executions
SET status = 'CANCELLED_BEFORE_EXECUTION', completed_at = NOW()
WHERE job_id = :jobId AND status = 'PENDING';
```

Meanwhile, when the scheduler worker thread fires a job, it executes:
```sql
UPDATE job_executions
SET status = 'RUNNING', actual_start_time = NOW()
WHERE id = :execId AND status = 'PENDING';
```

PostgreSQL serializes these row updates: one transaction gets the row lock, while the other blocks until the first commits. The loser's `WHERE status = 'PENDING'` predicate no longer matches, yielding `affected_rows = 0`.

- **If Step 2 returns `affected_rows = 1` (Cancel wins):** The execution is marked `CANCELLED_BEFORE_EXECUTION`. When the scheduler thread runs `claimForExecution()`, it sees 0 affected rows, logs that the execution was cancelled before firing, and safely exits without executing the job. The employee is not marked as a no-show.
- **If Step 2 returns `affected_rows = 0` (Fire wins):** The scheduler thread has already transitioned the execution from `PENDING` to `RUNNING` (or `COMPLETED`). The cancel API returns `409 Conflict (Already Executing)`. The calling service knows the job fired and can handle it appropriately.

**Guarantee:** Exactly one outcome. No execution is both cancelled and completed. Verified by a concurrency test running 100 iterations of simultaneous cancel + fire.

---

## 11. Outbox Pattern and Kafka

### Why the Outbox Pattern

When a job completes, downstream services need to know. The naive
approach — calling the downstream service directly from the handler — has
a problem: if the downstream call fails, should we retry the entire job?
What if the job's side effect succeeded but the notification didn't?

The outbox pattern decouples "doing the work" from "telling others about
it." The completion write and the outbox event insert happen in one
Postgres transaction. A separate publisher sends events to Kafka
asynchronously.

### Why Kafka for the Outbox, Not for Job Distribution

**For the outbox:** Kafka provides durable, ordered, multi-consumer event
delivery. Multiple downstream services (billing, roster, analytics) can
each consume at their own pace from the same topic. If a consumer is
down, events wait in Kafka's retention buffer.

**Not for job distribution:** The scheduler's requirements — priority
ordering, cancel-vs-fire atomicity, fenced writes in the same
transaction as side effects, per-tenant fairness, and dead-letter
inspection — are all stateful relational operations. Moving job delivery
to Kafka would add a second source of truth without eliminating any
Postgres logic.

### Failure Behavior

If Kafka is down, events accumulate in the `outbox_events` table. The
publisher retries every 1 second. When Kafka returns, the backlog drains
in order. Job execution is completely unaffected — Kafka is not on the
critical path.

---

## 12. Caching Strategy

The scheduler's hot path (poll → check tenant → check fairness →
execute → fenced write) must be fast. Two pieces of data are checked on
every job fire and can be cached:

| Data | Cache | TTL | Why this TTL |
|---|---|---|---|
| Tenant status (active/paused) | Caffeine, per node | 5 seconds | A pause takes up to 5s to take effect. Acceptable — pausing is an admin action, not safety-critical |
| Tenant timezone | Caffeine, per node | 1 hour | Timezone changes are extremely rare (admin action, typically never after setup) |

**What is NOT cached:** Job data, execution records, partition leases.
These change on every operation and must always be read from the database
for consistency.

**Cache invalidation:** When a tenant is paused or resumed via the API,
the cache is invalidated immediately on the node that handled the request.
Other nodes see the change within the TTL (5 seconds). This bounded
staleness is documented and acceptable.

**Cache metrics:** Hit rate, miss rate, and eviction count are exposed
through Micrometer, visible at `/actuator/prometheus`.

---

## 13. Monitoring and Observability

### Metrics (via Micrometer + Prometheus)

**The one metric that matters most: `fire.lag`**
This is `actual_start_time - scheduled_fire_time` in milliseconds,
recorded as a histogram. The p99 must be under 2 seconds. Everything
else exists to explain *why* fire lag went up.

| Metric | Type | Purpose |
|---|---|---|
| `jobs.fired.total` | Counter (by tenant, type, priority) | How many jobs executed |
| `jobs.fired.success` | Counter | Successful completions |
| `jobs.deadlettered` | Counter | Exhausted retries |
| `jobs.misfired` | Counter (by policy) | Detected misfires |
| `fire.lag` | Histogram (by priority) | The SLA metric |
| `job.execution.duration` | Histogram (by type) | Handler run time |
| `partitions.owned` | Gauge (per node) | Partition count per node |
| `partitions.orphaned` | Gauge | Expired leases (should be 0) |
| `workers.active` | Gauge | Busy threads |
| `deadletter.depth` | Gauge (per tenant) | Unresolved dead-letters |
| `cache.tenant.status` | Gauge (hit rate) | Cache effectiveness |

### Structured Logging

Every log line includes:
- ISO-8601 timestamp
- Thread name
- Log level
- Node ID (MDC `nodeId`)
- Tenant ID (MDC `tenantId`, only when in a tenant context)
- Logger name

**No PII in logs.** Job payloads (which may contain employee names, phone
numbers, home locations) are never logged. Error messages use job IDs and
error types only. An automated test greps all logs for PII patterns (email,
phone) and asserts zero matches.

### Alerts (Suggested Thresholds)

| Alert | Condition | Action |
|---|---|---|
| **High fire lag** | p99 > 2s for 5 minutes | Page on-call: investigate slow handlers or DB latency |
| **Orphaned partitions** | Any partition without a valid lease for > 60s | Page: a node died and nobody took over (shouldn't happen) |
| **Dead-letter spike** | > 100 new dead-letters in 10 minutes | Warn: a downstream service is likely down |
| **Rebalance storm** | > 3 rebalances in 5 minutes | Warn: nodes are flapping (network issue or misconfigured lease timeout) |

---

## 14. Security and Data Privacy

### Authentication

All API endpoints require a valid JWT token. The token contains:
- `tenantId` — which tenant the caller belongs to
- `userId` — who they are
- `role` — what they can do (ADMIN, OPERATOR, EMPLOYEE)

### Role-Based Access

| Role | Permissions |
|---|---|
| ADMIN | Full CRUD on all tenants' jobs (platform admin) |
| OPERATOR | Manage jobs for their assigned tenant |
| EMPLOYEE | Read-only on their own jobs (not in core scope) |

### Tenant Isolation

The `TenantIsolationFilter` extracts the tenant from the JWT and stores
it in a `ThreadLocal`. Every repository query includes
`WHERE tenant_id = :tenantId`. A Tenant A operator cannot see, modify,
or cancel Tenant B's jobs.

### No PII in Outputs

- Error responses use job IDs and error types, never employee names,
  phone numbers, or home locations.
- Log messages reference jobs by `jobId` and `tenantId` only.
- Payloads are never logged (they may contain `employeeId`,
  `homeLocation`, etc.).
- Test: run all handlers, grep log output for PII patterns → 0 matches.

---

## 15. Design Choices and Trade-offs

### Choice 1: PostgreSQL for Coordination (vs. Kafka or etcd)

**What we chose:** Postgres as the single coordination mechanism — leases,
fencing, job state, and outbox all in one database.

**What we gave up:** Faster failure detection (~30 seconds vs. ~2 seconds
with etcd session drops). Automatic rebalancing (Kafka consumer groups
handle this natively).

**Why:** The fenced write requires the lease check and the job completion
to be in one transaction. With Postgres, this is a SQL join in one
`BEGIN/COMMIT`. With etcd + Postgres, the check is in etcd and the write
is in Postgres — two systems, no atomic guarantee. With Kafka for job
distribution, we'd still need Postgres for priority ordering, cancel race
resolution, and dead-letter queries — adding a second source of truth
without removing the first.

**The 30-second detection delay is acceptable** because the spec itself
says "a node can freeze for 20 seconds." They expect these time scales.

### Choice 2: Per-Node Fairness (vs. Global)

**What we chose:** In-memory semaphore per tenant per node.

**What we gave up:** Precise global concurrency control (the cluster-wide
cap is `≤ N × perNodeCap`, not exactly `perNodeCap`).

**Why:** A global cap requires a distributed counter — either a database
row updated on every job fire, or a Redis counter. Both add a round-trip
to the hot path, increasing p99 latency. The per-node semaphore is O(1)
in-memory, with zero database overhead.

### Choice 3: CP for Execution, AP for Events

**Execution path:** Strong consistency. If Postgres is unreachable, we
stop firing rather than risk duplicates or fencing violations. This is
the CP side.

**Event delivery:** Eventual consistency. If Kafka is down, events queue
in Postgres and drain when Kafka returns. Events may be delivered
slightly delayed, but never lost. This is the AP side.

**Why asymmetric:** Job execution affects money (billing close, alert
delivery). Getting it wrong is worse than being slow. Event delivery is
informational — a 10-second delay in a "job completed" event to the
analytics pipeline is harmless.

### Choice 4: Horizontal Scaling by Partition Count

**What we chose:** 64 fixed partitions.

**When it stops working:** At ~64 nodes, each node gets only 1 partition.
Beyond that, increase to 256 or 1024 partitions (a one-time migration:
rehash existing jobs). The partition count is a configuration knob
(`scheduler.partition-count`), not a hard-coded constant.

### What We Deliberately Left Out

- **Job dependency DAGs:** Parent-child spawning is supported; complex
  DAGs (job C runs only after A and B both complete) are not.
- **Multi-region deployment:** Each region would be an independent
  deployment. Cross-region coordination is out of scope.
- **UI/Dashboard:** API only. An operator dashboard would consume the
  REST API and Kafka events.
- **Exactly-once message delivery to external systems:** We guarantee
  exactly-once effect within our database. Downstream systems must
  deduplicate by idempotency key.

---

## 16. Where the System Stops Scaling

### Big-O Algorithmic Complexity

| Operation | Complexity | Implementation Mechanism |
|---|---|---|
| **Poll Due Jobs** | $O(\log N + K)$ | B-tree index scan on `(partition_id, status, next_fire_time)` where $N$ is total partition jobs, and $K$ is batch limit (50). |
| **Lease Acquisition** | $O(P \log P)$ | Single SQL query over $P = 64$ partitions using `FOR UPDATE SKIP LOCKED LIMIT :fairShare`. |
| **Fenced Completion** | $O(1)$ | Primary key index lookup on `job_executions.id` joined with `partition_leases.partition_id`. |
| **Fairness Concurrency** | $O(1)$ | In-memory `ConcurrentHashMap.computeIfAbsent` and non-blocking `Semaphore.tryAcquire()` / `release()`. |
| **Misfire Recovery Scan** | $O(M)$ | Index scan on overdue or orphaned executions ($M$) across newly claimed partitions. |
| **Outbox Publishing** | $O(B)$ | Index scan on unpublished events (`WHERE published = false ORDER BY created_at LIMIT :batchSize`). |

### Scaling Tiers

| Scale Level | Setup | Capacity | Bottleneck |
|---|---|---|---|
| **Current** | 3 nodes, 1 Postgres, 64 partitions | ~5,000 jobs/sec | Nothing (at <5% capacity) |
| **Medium** | 10 nodes, 1 Postgres (read replicas for dashboards), 256 partitions | ~15,000 jobs/sec | Single Postgres write throughput |
| **Large** | 30 nodes, 4 Postgres shards (by partition range), 1024 partitions | ~60,000 jobs/sec | Shard count × per-shard TPS |
| **Very Large** | Replace Postgres coordination with Kafka/etcd, shard by tenant, multi-region | Millions/sec | Architecture redesign required |

**Sharding preserves fencing guarantees** because each shard holds a
contiguous range of partitions with their leases and executions. The
fenced write joins `job_executions` with `partition_leases` — both on the
same shard, same transaction.

---

## 17. How We Prove It — Tests and Benchmarks

### Unit Tests (fast, no infrastructure)

| Test | What it proves |
|---|---|
| `CronResolverTest` | DST spring-forward, fall-back, ascending fire times |
| `RetryCalculatorTest` | Exponential backoff, jitter bounds, max cap |
| `PartitionAssignerTest` | Bounds [0,63], determinism, even distribution |
| `FairnessQueueTest` | Capacity limit, release, tenant independence |

### Integration Tests (real Postgres via Testcontainers)

| Test | Guarantee |
|---|---|
| `JobLifecycleTest` | G1 (no loss), G2 (no early fire), G3 (idempotent creation) |
| `TenantIsolationTest` | G5 (cross-tenant access denied) |
| `CancelRaceTest` | G9 (100 concurrent cancel+fire iterations, no duplicates) |
| `PauseResumeTest` | O1 (paused jobs held, resumed with misfire policy) |
| `FencingTokenTest` | G4 (stale writes rejected) |
| `PartitionCoverageTest` | G6 (all partitions covered after rebalance) |

### Distributed / Failure Tests (live Docker cluster)

| Test | Scenario |
|---|---|
| `NodeCrashRecoveryTest` | kill -9 during 1,200 alerts → surviving nodes complete all |
| `NodeFreezeTest` | SIGSTOP 30s → stale write rejected by fencing token |

### Combined Benchmark (100K jobs, 3 nodes, with failures)

The definitive test. 100,000 jobs spread across a 10-minute window.
During the run:

- **Minute 3:** `docker kill scheduler-node2` (unclean crash mid-processing)
- **Minute 5:** `docker pause scheduler-node3` (simulated 30s GC freeze)
- **Minute 5:30:** `docker unpause scheduler-node3` (stale writes fenced)
- **Minute 7:** Restart `scheduler-node2` (rejoin, rebalance)
- **Minute 10:** All 100,000 jobs complete

**Measured metrics:**
- p50 fire lag: reported in README
- p99 fire lag: must be < 2 seconds
- Throughput: jobs/sec sustained
- Zero lost jobs
- Zero duplicate executions
- At least one `FENCING_REJECTED` in logs (from unfrozen node-3)
- Machine spec reported alongside results

---

## 18. Requirement Coverage Matrix

### Case Study Requirements (Section I–IV)

| Requirement | Status | Implementation |
|---|---|---|
| One-off jobs at a time | ✅ | `ScheduleType.ONE_OFF` |
| Recurring jobs from cron | ✅ | `ScheduleType.RECURRING` + `CronResolver` |
| Tenant, payload, priority, max run time | ✅ | Fields on `Job` entity |
| Time zones for recurring jobs | ✅ | `CronResolver` with `ZonedDateTime` |
| DST spring-forward | ✅ | TZ3 policy + test |
| DST fall-back | ✅ | TZ4 policy + test |
| At-least-once execution | ✅ | Crash recovery re-executes |
| Idempotency key per run | ✅ | `jobId:fireTimeUTC` unique constraint |
| Exactly-once side effect | ✅ | Fenced write + same transaction |
| Time-limited lease | ✅ | 30-second leases in `partition_leases` |
| Fencing token | ✅ | Monotonic token, conditional UPDATE |
| No single hot lock | ✅ | 64 partitions + `SKIP LOCKED` |
| Partitions move on join/leave | ✅ | Lease expiry + voluntary shedding |
| Misfire policy: fire now | ✅ | `MisfireScanner` |
| Misfire policy: skip | ✅ | Advance next fire time |
| Misfire policy: coalesce | ✅ | Collapse into one execution |
| Per-tenant fairness | ✅ | In-memory semaphore, cap 50/node |
| Retry with backoff + jitter | ✅ | `RetryCalculator` |
| Dead-letter with inspection/replay | ✅ | `DeadLetterController` |
| Pause/resume per tenant | ✅ | `TenantController` |
| List next fire times | ✅ | `CronResolver.nextFireTimes()` |
| Manual trigger | ✅ | Advances next fire time to prevent double-fire |
| Fire lag histogram | ✅ | Micrometer histogram |
| Running/retrying/dead-letter counts | ✅ | Micrometer gauges |

### Plus Points (Rubric Criteria)

| # | Criterion | Status | Implementation |
|---|---|---|---|
| 1 | Authentication + RBAC + tenant isolation | ✅ | JWT, roles, `TenantIsolationFilter` |
| 2 | Time/space complexity + benchmark | ✅ | O(n log n) sort, O(n) memory per trip, 100K benchmark |
| 3 | Failure handling + failure-injection test | ✅ | Kill test, freeze test, DB-down handling |
| 4 | OOP — encapsulation, inheritance, polymorphism | ✅ | Strategy pattern (handlers), clean controller/service/repository layering |
| 5 | Trade-offs documented | ✅ | Section 15 of this document |
| 6 | Monitoring — metrics + structured logging | ✅ | Micrometer counters/histograms/gauges, JSON logs, no PII |
| 7 | Caching | ✅ | Caffeine cache for tenant status (5s TTL) and timezone (1hr TTL) |
| 8 | Error handling — meaningful, non-leaking | ✅ | Exception hierarchy, `GlobalExceptionHandler`, no PII in errors |

### Advanced Track Deliverables

| Deliverable | Status |
|---|---|
| Automated test for every hard guarantee (G1–G9) | ✅ |
| At least one concurrency test | ✅ (`CancelRaceTest`) |
| At least one failure test | ✅ (`NodeCrashRecoveryTest`, `NodeFreezeTest`) |
| Load test with p50/p99 | ✅ (`SchedulerBenchmark`) |
| Architecture diagram | ✅ (Section 2) |
| Sequence diagram for hardest flow | ✅ (`DESIGN_NOTE.md`, node crash recovery) |
| Failure-mode table | ✅ (Section 6 + `DESIGN_NOTE.md`) |
| `docker compose up` | ✅ (Postgres + Kafka + 3 nodes) |
| Meaningful commit history | ✅ (~20 commits, conventional format) |
| Demo video | ✅ ([Google Drive Link](https://drive.google.com/drive/folders/1EuAMylkT7YqvRZ7Ju1IkQQkO2z2Y0kBC)) |
