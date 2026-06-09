# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-06-09

### Added
- **Asynchronous Queue Worker (`worker.ts`):** Runs a background consumer daemon using blocking pop (`BLPOP`) operations, with graceful shutdown signal handlers and a Dead-Letter Queue (DLQ) enqueuing mechanism.
- **Atomic sliding-window rate limiter (`limiter.ts`):** Implemented using a Redis Sorted Set (ZSET) and a custom Lua script to guarantee thread-safe checks across distributed nodes.
- **Twilio SMS service (`twilio.ts`):** Wraps the Twilio Node helper SDK with custom error handling, actionable debugging hints, and automatic sandbox simulation mode.
- **Alert Publisher Client (`publisher.ts`):** Used by enqueuing services to hash event attributes into unique fingerprints and push payloads onto the queue.
- **Test Pipeline Simulation (`test-pipeline.ts`):** Simulates incident storms by pushing bursts of concurrent duplicate events, with automatic Redis health checking and worker detection.
- **Infrastructure Configuration (`docker-compose.yml`):** Spins up a Redis v7.2 database container configured with health checks and data volumes.
- **Configuration & Validation Layer (`config.ts`):** Parses environment variables, validates phone formats, and implements holistic mock detection logic.

### Technical Notes
This initial release establishes a decoupled architecture designed to protect alerting gateways from incident storms. The rate-limiting logic uses a single-threaded Redis Lua transaction that prunes, counts, and registers items within one database operation, eliminating network race conditions. The system relies on a dual-connection pattern (separating blocking queue operations from non-blocking writes) and runs in an interactive simulation mode out-of-the-box, falling back to mock dispatches if credentials are not configured.
