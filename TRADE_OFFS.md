# Engineering Trade-Offs & Limitations — MoveInSync Scheduler

This document explicitly details the intentional design trade-offs made in the MoveInSync Distributed Trip Job Scheduler, their rationale, and known operational boundaries.

---

## 1. In-Memory Concurrency Semaphores vs. Distributed State (Redis/etcd)

- **Decision:** Multi-tenant fairness limits (`FairnessQueue`) are enforced via in-memory Java `Semaphore` instances rather than a centralized Redis or etcd cluster.
- **Benefits:**
  - **Zero latency overhead on the hot path:** Thread slot acquisition and release take sub-microsecond time with zero network I/O.
  - **Reduced operational complexity:** No external caching cluster to manage, monitor, or failover.
- **Trade-off:**
  - The concurrency limit (`scheduler.max-concurrent-per-tenant = 50`) is per node. In a 3-node cluster, a single tenant can theoretically run up to $3 \times 50 = 150$ concurrent jobs if their workload is evenly distributed across partitions.
  - For MoveInSync's scale, this provides predictable upper bounds per machine while preventing any single tenant from exhausting an individual node's worker pool.

---

## 2. 5-Second Pause Propagation Window via Caffeine Cache

- **Decision:** Tenant pause state is cached in-memory with a 5-second TTL (`TenantService.statusCache`).
- **Benefits:**
  - Saves a database query on every single job execution across hundreds of thousands of jobs.
  - Enables sub-millisecond job dispatch latency.
- **Trade-off:**
  - When an administrator calls `POST /api/tenants/{id}/pause`, local cache on that node is invalidated immediately, but other cluster nodes may continue firing that tenant's scheduled jobs for up to 5 seconds until their local Caffeine cache expires.
  - This 5-second eventual consistency window is documented in the API specification and is completely acceptable for roster generation and cab alerts.

---

## 3. Fixed 64-Partition Sharding Model

- **Decision:** Partition count is configured to 64 partitions (`scheduler.partition-count = 64`).
- **Benefits:**
  - Low coordination overhead: 64 rows in `partition_leases` allows lightweight heartbeat and rebalance scans.
  - In a 3-node cluster, nodes evenly hold 22, 21, and 21 partitions.
- **Trade-off:**
  - The maximum optimal cluster size is 64 nodes (1 partition per node). Beyond 64 nodes, extra nodes will remain idle with 0 partitions.
  - Increasing partition count (e.g. to 256 or 1024) requires running a one-time migration to recompute `partition_id` for existing pending jobs.

---

## 4. Single PostgreSQL Primary as Coordination Point

- **Decision:** Coordination, partition leasing, and job state reside in a single shared PostgreSQL instance rather than using Raft (Consul/etcd) or ZooKeeper.
- **Benefits:**
  - **Single transactional boundary:** Completing a job, incrementing or validating fencing tokens, and writing outbox events occur in one atomic ACID commit.
  - No distributed 2-phase commit (2PC) or dual-write anomalies.
- **Trade-off:**
  - Database availability: If PostgreSQL becomes completely unreachable, all nodes enter a safe pause/reconnect loop. In production, this is mitigated via AWS Aurora PostgreSQL Multi-AZ or Patroni HA clusters.

---

## 5. Elevated Benchmark Pools vs Production Defaults

- **Decision:** Benchmark ran with elevated worker pool (`50`) and batch size (`150`), while production defaults remain `20` and `50`.
- **Rationale:**
  - During the 100K chaos benchmark, Node 2 was killed (`kill -9`) and Node 3 was frozen (`docker pause`), forcing Node 1 to temporarily handle the entire 64-partition load alone.
  - Elevated pools were configured via Docker Compose environment variables to ensure single-node capacity during extreme chaos without altering safe production defaults in `application.yml`.

