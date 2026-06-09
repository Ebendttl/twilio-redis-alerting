# Alerting Pipeline Architecture

This document details the architectural decisions, failure mode mitigations, scaling profiles, and deliberate engineering tradeoffs of the `twilio-redis-alerting` service.

---

## 1. System Design Decisions

### Redis List (`BLPOP`) vs. Redis Pub/Sub
The primary alternative for enqueuing alert payloads is Redis Pub/Sub. While Pub/Sub offers low-latency, real-time message broadcasting, it operates on a "fire-and-forget" paradigm. It maintains no message state. If the worker daemon crashed, restarted, or underwent deployment during an active outage, all alerts enqueued during that window would be lost. Additionally, Pub/Sub lacks backpressure control; it broadcasts messages simultaneously to all active subscribers, risking worker exhaustion during high-volume event storms.

We chose Redis Lists enqueued with `RPUSH` and consumed via `BLPOP`. A list guarantees that enqueued messages persist within Redis until a worker fetches them. By using `BLPOP` (Blocking Left Pop), we implement a pull-based worker model. The blocking connection goes to sleep when the list is empty, eliminating CPU-intensive polling loops. If an alert storm hits, the list acts as a buffer, allowing workers to drain messages sequentially at a controlled rate, preventing downstream SMS gateway choking.

### Sliding-Window Log vs. Token Bucket
A Token Bucket or Leaky Bucket algorithm uses a central counter representing available "tokens" that refresh over time. The alternative would be to track limits using simple string keys and an increment counter (e.g. `INCRBY` on `rate_limit:fingerprint:minute_window`). However, fixed-window incrementing suffers from "boundary spikes." An attacker or a malfunctioning system can publish their entire limit (e.g., 3 alerts) at the end of window `N` (e.g., at 1:59 AM) and another full limit at the start of window `N+1` (e.g., at 2:00 AM). Under this condition, 6 alerts are dispatched in a sub-second span, violating the rate limit rules.

We chose the Sliding-Window Log algorithm, implemented via Redis Sorted Sets (ZSETs). By storing every request timestamp as a score in the set, we evaluate the exact number of requests that occurred in the preceding rolling window (e.g., `now - 60 seconds` to `now`). While storing individual timestamps consumes more Redis memory than maintaining a simple counter, the sliding window prevents boundary double-bursting and provides accurate, real-time cooldown feedback, allowing the worker to report the exact number of seconds until a rate limit resets.

### Atomic Lua Script vs. TypeScript Application-Level Locking
Performing rate-limit calculations in TypeScript requires enqueuing elements, checking the count, and applying a TTL using separate commands. In a distributed environment with multiple parallel workers, this introduces a race condition. If two worker daemons query the ZSET count simultaneously, both see a count of 2 (under a limit of 3) and both approve and insert their respective alerts. The limit of 3 is breached because the "check-then-act" sequence is not thread-safe across multiple nodes.

We chose an atomic Lua script registered via `ioredis.defineCommand`. Redis runs all Lua scripts in a single-threaded execution thread. This guarantees that no other client commands can execute while the Lua script is running. The entire sequence—pruning expired records, counting active entries, conditionally inserting the new alert, and applying key expiration—runs as a single transaction. This prevents concurrency drift without requiring distributed locking mechanisms (like Redlock) that introduce network latency.

### Dual Redis Connection Strategy vs. Single Connection
A naive Redis client implementation uses a single TCP connection for all database commands. However, enqueuing or polling via `BLPOP` blocks the connection. If the worker daemon attempts to write to a dead-letter queue, publish a metric, or check rate limits on the same connection currently waiting on a `BLPOP` call, the client will freeze, causing commands to time out or queue up.

We implemented a dual-connection strategy. The worker daemon initializes `redisBlocking` solely for enqueuing the blocking `BLPOP` loop, and `redisMain` for standard, non-blocking commands (like executing Lua scripts and writing to the dead-letter queue). This guarantees that enqueuing and rate-limit checks run in parallel, avoiding worker deadlocks.

---

## 2. Failure Mode Analysis

| Failure Scenario | Current Behavior | Recommended Production Mitigation |
| :--- | :--- | :--- |
| **Redis host crashes mid-processing** | The blocking socket throws a connection error. The loop catches the error, sleeps for 2 seconds, and then attempts `BLPOP` again. `ioredis` handles automatic TCP reconnection in the background. | Deploy Redis in a High Availability (HA) cluster configuration with Sentinel or replication. Configure the `REDIS_URL` with a connection string supporting failover hosts. |
| **Twilio API returns a `429 Too Many Requests`** | The `twilio.ts` service catches the error, logs the API failure code, and throws an exception. The worker logs `[PROCESSING FAILED]` and moves to the next message. The failed alert is lost. | Implement an exponential backoff retry mechanism inside `worker.ts` for transient HTTP errors, or enqueue failed delivery payloads to a retry queue with a delay (e.g. using a sorted set for scheduled retries). |
| **Worker process receives a `SIGKILL`** | The process is terminated instantly by the OS. The alert currently being processed is lost. The database connection is closed abruptly. | Migrate the queue from a Redis List to **Redis Streams** and consume alerts using **Consumer Groups (`XREADGROUP`)**. Redis Streams track enqueued messages inside a Pending Entries List (PEL). If a worker dies before calling `XACK` (acknowledge), another worker can claim and re-process the orphaned message. |
| **Malformed JSON enters the queue** | `JSON.parse` throws an error inside the worker. The worker logs a `[DEAD LETTER]` error, writes the raw string payload to the `alerting:dead-letter` list queue, and continues processing. | Implement schema validation (e.g. using `zod`) on enqueued payloads to sanitize fields before processing. Monitor the size of `alerting:dead-letter` and configure alerts to notify developers if it grows. |
| **Network partition between Worker and Redis** | The TCP connection drops. The worker error handlers log the connection state, and the polling loop blocks, retrying every 2 seconds. The worker ceases processing queue elements until the partition heals. | Set up a multi-region deployment. Run duplicate workers in different availability zones enqueuing from replicated Redis databases. Use local in-memory fallbacks if the network is down. |

---

## 3. Scaling Characteristics

### 0 to 1,000 Alerts/Day (Current Scale)
At this scale, the current architecture runs with a single Node.js worker and a single Redis instance (even on a shared base-tier VM). Resource utilization is negligible. Redis memory footprint is minimal because keys automatically clean up via `PEXPIRE` TTLs within 60 seconds.

### 1,000 to 100,000 Alerts/Day (Medium Scale)
* **Bottleneck:** The worker process may block on Twilio HTTP API roundtrips (150ms–300ms per request). A single worker thread running sequentially will experience queue lag during burst storms.
* **Upgrade Path:**
  1. Increase worker concurrency by spawning multiple instances of the worker daemon enqueuing from the same Redis queue.
  2. Implement an asynchronous task queue wrapper inside the worker (e.g. using `Promise.all` up to a concurrency limit) to handle network dispatches concurrently.
  3. Ensure Redis enqueues are distributed across connections.

### 100,000+ Alerts/Day (High Scale)
* **Bottleneck:** A single Redis instance hosting the ZSETs may face memory constraints if thousands of unique fingerprints are tracked concurrently. High ZSET write volumes could impact single-threaded CPU limits.
* **Upgrade Path:**
  1. Migrate to a Redis Cluster. To prevent slot mismatch errors (`CROSSSLOT`) when running Lua scripts on multiple keys, modify keys to use hash tags (e.g. `{rate_limit:fingerprint}`).
  2. Transition from Redis Lists to Redis Streams (`XREADGROUP`) to distribute message loads across multiple worker nodes.
  3. Set up a Twilio status callback webhook to handle delivery status tracking asynchronously, reducing worker connection hold times.

---

## 4. Known Tradeoffs

1. **Memory Complexity vs. Rate Accuracy:**
   By choosing Sliding-Window Logs (ZSETs), we store every alert's UUID and timestamp. During a high-throughput incident, if a fingerprint receives 10,000 alerts per minute, Redis will store 10,000 elements in a single ZSET. A Token Bucket, by contrast, uses a single string counter (consuming constant memory). We trade off higher memory usage in Redis for the precision of sliding limits and instant cooldown tracking.
2. **At-Least-Once Delivery vs. System Complexity:**
   Using a Redis List (`BLPOP`) is simple and fast, but it is not transactional. If the worker process receives a `SIGKILL` while executing `twilio.sendSms`, the message is lost forever. We accepted this tradeoff to avoid the complexity of Redis Streams, Pending Entries List tracking, and duplicate delivery deduplication logic.
3. **Mock Mode Fallback vs. Configuration Safety:**
   The configuration manager transparently switches to Mock Mode if placeholders are detected. While this creates a frictionless local experience, a developer might accidentally deploy to production with placeholder credentials, failing to send real SMS alerts without triggering an immediate crash. We prioritize local cold-cloning usability over strict production-only validation.
