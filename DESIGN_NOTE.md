# Architecture Design Note — MoveInSync Distributed Scheduler

## 1. Partition Distribution: `hash(jobId)` vs `hash(tenantId)`

A key design choice in this scheduler is how jobs are assigned to partitions. We deliberately use:

```java
partitionId = Math.floorMod(jobId.hashCode(), partitionCount); // partitionCount = 64
```

### Why `hash(jobId)` and NOT `hash(tenantId)`?

1. **Even Cluster Utilization:** In an enterprise transport system, tenant job volume is highly skewed (e.g., Tenant A may schedule 80,000 trips while Tenant B schedules 2,000). If we partitioned by `hash(tenantId)`, Tenant A's entire workload would be pinned to a single partition, and therefore executed by a single cluster node. With `hash(jobId)`, Tenant A's jobs are evenly distributed across all 64 partitions and shared across all 3 nodes.
2. **Elimination of Hot Partitions:** No single database row lock or partition queue can become a system-wide bottleneck.
3. **Fairness Maintained via In-Memory Semaphores:** Multi-tenant fairness is decoupled from storage partitioning. Rather than isolating tenants into separate database partitions, the `FairnessQueue` enforces per-tenant concurrency limits dynamically across all worker threads on each node.

---

## 2. Distributed Coordination via Database Leases & Fencing Tokens

To avoid split-brain execution and prevent stale zombie workers from committing phantom writes during long GC pauses or network partitions, the scheduler uses a two-tier mechanism:

### Partition Leases (Native PostgreSQL Clock)
- Partition ownership is tracked in `partition_leases`:
  - `partition_id` (0..63)
  - `node_id` (e.g. `node-1`)
  - `fencing_token` (monotonically increasing `BIGINT`)
  - `lease_expires` (`TIMESTAMP WITH TIME ZONE`)
- Leases are acquired and renewed exclusively using PostgreSQL database clock (`NOW() + INTERVAL '30 seconds'`). JVM clock skew across servers never affects lease correctness.
- Acquisition uses `FOR UPDATE SKIP LOCKED` with fair share limit `ceil(64 / active_nodes)`, preventing thundering herds and contention during rolling restarts.

### Monotonic Fencing Tokens
- Whenever a partition changes ownership (failover, startup, or rebalance), its `fencing_token` is incremented atomically in PostgreSQL:
  ```sql
  UPDATE partition_leases
  SET node_id = :myNodeId,
      fencing_token = fencing_token + 1,
      lease_expires = NOW() + (:timeout || ' seconds')::INTERVAL
  WHERE partition_id = :partitionId;
  ```
- When a worker finishes executing a job, `FencedWriter` executes a conditional update checking that the fencing token has not changed:
  ```sql
  UPDATE job_executions
  SET status = 'COMPLETED', completed_at = NOW()
  FROM partition_leases pl
  WHERE job_executions.id = :execId
    AND pl.partition_id = :partitionId
    AND pl.fencing_token = :myFencingToken;
  ```
- If a frozen node wakes up after its lease expired and tries to complete a job, `affected_rows = 0`. The database transaction rolls back, rejecting the stale write.

---

## 3. Transactional Outbox Pattern

To achieve exactly-once effect without distributed two-phase commits (2PC):
1. Handlers do **not** publish directly to message brokers or external HTTP APIs inside the transaction.
2. Handlers write business state and an `outbox_events` record within the **same local PostgreSQL transaction** as the fenced completion check.
3. If the fencing token check fails, the outbox record rolls back atomically, ensuring external systems never receive the event.
4. A dedicated background thread (`OutboxPublisher`) polls published events and delivers them to Kafka with producer idempotence (`enable.idempotence=true`).

---

## 4. Multi-Tenant Fairness and Thread Pool Isolation

- **Priority Routing:** Two decoupled thread pools ensure urgent operational work is never starved:
  - `criticalPool` (4 threads): Reserved exclusively for `CRITICAL` priority jobs (e.g., driver assignment, immediate pickup alerts).
  - `workerPool` (20 threads default, 50 in benchmark): Serves `HIGH`, `NORMAL`, and `LOW` priority jobs.
- **FairnessQueue:** An in-memory concurrency semaphore per tenant prevents noisy tenants from monopolizing worker threads.
- **Caffeine Metadata Caching:** Tenant pause state is cached with a 5-second TTL (`isTenantPaused`), eliminating redundant database roundtrips on high-frequency poll cycles.
