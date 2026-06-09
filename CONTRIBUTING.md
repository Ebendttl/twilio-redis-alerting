# Contributing to Twilio Redis Alerting

This document outlines the local development setup, testing workflows, and engineering guidelines for contributing to this project. This project is built using TypeScript, Node.js, and Redis.

---

## Local Development Setup

To get your development environment running locally, execute these steps in sequence:

### 1. Clone and Install Dependencies
Clone the repository and install the locked package dependencies:
```bash
git clone https://github.com/Ebendttl/twilio-redis-alerting.git
cd twilio-redis-alerting
npm install
```

### 2. Configure Environment Variables
Copy the template configuration file to create your local environment file:
```bash
cp .env.example .env
```
By default, the `.env` template is configured to run in **Mock (Simulation) Mode** without needing real Twilio credentials. If you want to use the live SMS gateway, edit the `.env` file and replace the placeholder values with your active Twilio credentials.

### 3. Spin Up Infrastructure
Launch the local Redis database container. The compose file includes a health check to ensure Redis is fully ready before accepting traffic:
```bash
docker compose up -d
```

### 4. Build and Start the Worker
Compile the TypeScript code and launch the background consumer daemon:
```bash
npm run build
npm run worker
```

**Expected Startup Output:**
```text
[CONFIG WARNING] ALERT_RECIPIENT_PHONE_NUMBER is set to the placeholder value "+1234567890". SMS will not be delivered in live mode.
  Update your .env file with a real phone number.

[WORKER] Alert Worker Daemon initialized.
[WORKER] Monitoring queue: "alerting:queue"
[WORKER] SMS Mode: SIMULATED
[WORKER] Mode reason: TWILIO_ACCOUNT_SID is not a valid 34-char SID (got "ACXXXX...")
[WORKER] Recipient: +1234567890
[WORKER] Rate limit: Max 3 per 60s per fingerprint
[WORKER] Dead-letter queue: "alerting:dead-letter"
```

---

## Running the Test Pipeline

To verify rate-limiting and asynchronous processing, run the test pipeline in a separate terminal:

```bash
npm run test-pipeline
```

**Expected Test Output:**
```text
[TEST-RUNNER] Connecting to Redis...
[TEST-RUNNER] Cleaned up existing rate limits and queues.

[TEST-RUNNER] Publishing Alert Burst: 5 identical database failures...
  -> Queued [Burst #1] ID: 278e3fa2-0052-42ba-aa58-c70571c99311
  -> Queued [Burst #2] ID: 6e14d64b-aa24-4aa0-9cb3-fd178d1320f5
  -> Queued [Burst #3] ID: 81462b81-872d-4fed-8a2e-52adef480764
  -> Queued [Burst #4] ID: a199b846-fe55-4a02-9d6e-ef6b7810e338
  -> Queued [Burst #5] ID: d8b6138a-5a5d-4f9d-87ae-9949cab63bca

[TEST-RUNNER] Publishing 1 alternative alert (OOM) with different fingerprint...
  -> Queued [OOM] ID: 72fde672-aeb9-4f8f-8bce-21ef434c193e

[TEST-RUNNER] Simulation complete.
Expected results in the worker terminal:
  1. Three database alerts dispatched (SMS sent).
  2. Two database alerts suppressed (rate-limited).
  3. One OOM alert dispatched (separate fingerprint, not rate-limited).
```

---

## Project Structure

The codebase is organized into modular TypeScript files inside the `src/` directory:

| File | Responsibility Description |
| :--- | :--- |
| `src/types.ts` | Declares shared TypeScript interfaces for alerts, rate limits, and configuration shapes. |
| `src/config.ts` | Performs environment variable parsing, E.164 phone verification, and holistic mock mode detection. |
| `src/limiter.ts` | Registers and executes the atomic Redis Lua script containing the sliding-window rate limit checks. |
| `src/twilio.ts` | Interfaces with the Twilio Node SDK, translating error codes to readable hints and running mock simulations. |
| `src/worker.ts` | Runs the continuous queue polling worker utilizing blocking pops, dead-letter queues, and graceful shutdown hooks. |
| `src/publisher.ts` | Defines the client publisher used by microservices to hash fingerprints and enqueue alerts. |
| `src/test-pipeline.ts` | Automates pipeline cleanups and sends test bursts to verify suppression logic under load. |

---

## Engineering & Contribution Standards

To keep the codebase production-grade, adhere to these standards:

### 1. TypeScript Strict Mode
* Do not use `any`. Define exact type interfaces in `src/types.ts`.
* Keep compiler options set to `"strict": true` in `tsconfig.json`.

### 2. Error Handling
* Do not swallow errors or log raw exceptions with `console.log(err)`.
* Always use typed exception handling and extract specific metadata (e.g., Twilio API code blocks or Node system error codes).
* Write actionable recovery hints when catching known exceptions.
* Route unparseable, corrupted payloads to the designated dead-letter queue (`alerting:dead-letter`) rather than letting worker loops fail silently.

### 3. Environment Variables
* Declare all configuration variables in `.env.example`.
* Add explicit inline documentation describing how and where to obtain credentials.
* Never commit real production tokens or `.env` files to git.
