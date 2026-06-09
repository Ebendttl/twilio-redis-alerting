# Architecting a Resilient Distributed Alerting Pipeline with Redis Sliding-Window Rate Limiting and Twilio SMS

It is 2:00 AM. A transient network partition has just severed your web server fleet from your primary PostgreSQL cluster. Within seconds, the database connection pool exhausts. The application servers begin spawning thousands of database timeout exceptions per second. In a naive alerting architecture, each exception triggers a direct, synchronous call to your SMS gateway. 

The consequences are immediate and catastrophic:
1. **API Rate Limiting:** The sheer volume of concurrent requests triggers Twilio API throttling, queueing notifications and delaying critical, unrelated status updates.
2. **Financial Bleed:** A single unthrottled loop executing thousands of SMS dispatches can drain your company's Twilio credit balance in minutes.
3. **On-Call Paralysis:** An engineer wakes up to a phone vibrating continuously with thousands of identical text messages. This is the definition of "alert fatigue"—it hides the root cause and slows down the time to resolution.

To survive an incident storm, you need a decoupled alerting system that throttles warnings dynamically while ensuring no critical data is lost. This tutorial walks you through building a production-ready, distributed alerting pipeline in TypeScript. We will construct an asynchronous worker pool using a durable **Redis Queue**, implement a highly precise **Sliding-Window Log rate-limiter** via an **atomic Redis Lua script**, and connect it to the **Twilio SMS API** with built-in format validation and sandbox simulation fallbacks.

---

## Architecture Flow

Below is the ASCII diagram of our alerting pipeline. By separating alert publishers (your microservices) from the actual SMS delivery worker pool, we isolate downstream gateways from high-volume spikes.

```text
+-----------------------+      +-------------------------+
|   Auth Microservice   |      |  Database Monitor Job   |
+-----------+-----------+      +------------+------------+
            |                               |
            | (RPUSH alert JSON)            | (RPUSH alert JSON)
            v                               v
+--------------------------------------------------------+
|             Centralized Broker: Redis Queue            |
|                Key: "alerting:queue"                   |
+---------------------------+----------------------------+
                            |
                            | (BLPOP pops alert immediately)
                            v
+---------------------------+----------------------------+
|             Alert Processing Worker Daemon             |
|                                                        |
|  1. Executes Lua script: checkRateLimit(fingerprint)    |
+---------------------------+----------------------------+
                            |
      +---------------------+---------------------+
      | (If Allowed: < limit)                     | (If Throttled: >= limit)
      v                                           v
+-----+-----------------+                   +-----+-----------------+
| Twilio SMS Gateway    |                   | Mock Console Logger   |
| (Dispatched to Phone) |                   | (Suppressed message)  |
+-----------------------+                   +-----------------------+
```

### Key Architectural Decisions

1. **Redis List (`BLPOP`) vs. Pub/Sub:** 
   Standard Redis Pub/Sub is a "fire-and-forget" protocol; it does not persist messages. If your worker pool restarts or crashes during an incident, all alerts published in that window are lost. Using a Redis List as a queue provides durability. The worker daemon uses `BLPOP` (Blocking Left Pop), which blocks the connection and waits. When an item is pushed onto the queue, Redis immediately forwards it to the worker, achieving sub-millisecond dispatch latency without CPU busy-waiting.
2. **Sliding-Window Log vs. Token Bucket:**
   Token Bucket algorithms work well for global API throttling but suffer from "boundary spikes"—where double the limit can leak through during the boundary reset window. The **Sliding Window Log** tracks the exact timestamp of every request in a rolling window (using a Redis Sorted Set). This provides precise cooldown tracking, letting us notify engineers exactly when the rate-limiting block will clear.
3. **Atomic Lua Script Execution:**
   In a distributed system with multiple worker instances, a "read current count, then conditionally increment" pattern introduces critical race conditions (e.g. two workers allowing a fourth alert simultaneously). Registering our logic as a Lua script using `ioredis.defineCommand` ensures that the entire prune-count-insert-expire sequence runs atomically in a single execution thread inside Redis.

---

## Prerequisites

To follow this tutorial, ensure your local environment contains:
* **Node.js**: v20.x or higher
* **Docker**: v20.10.x or higher (For running Redis in a clean container)
* **Twilio Account**: Optional (The codebase automatically falls back to an interactive console mock mode if placeholders are detected in your `.env`)

### Project Dependencies

Dependencies are pinned to exact versions inside `package.json` to guarantee reproducible builds:

| Package | Version | Purpose |
| :--- | :--- | :--- |
| `ioredis` | `5.4.1` | High-performance Redis driver supporting custom Lua commands. |
| `twilio` | `5.13.1` | Official Twilio helper SDK for Programmable SMS. |
| `dotenv` | `16.4.5` | Environment variables loader. |
| `tsx` | `4.7.2` | High-speed TypeScript execution engine. |

---

## Step-by-Step Implementation

*All commands must be executed from the repository root directory unless otherwise specified.*

### Step 1: Project Configuration

Create `package.json` to lock down dependencies and establish scripts:

```json
{
  "name": "twilio-redis-alerting",
  "version": "1.0.0",
  "description": "Distributed Alerting Pipeline with Redis Pub/Sub, Sliding-Window Rate Limiting, and Twilio SMS",
  "main": "dist/worker.js",
  "scripts": {
    "build": "tsc",
    "worker": "tsx src/worker.ts",
    "publisher": "tsx src/publisher.ts",
    "test-pipeline": "tsx src/test-pipeline.ts"
  },
  "dependencies": {
    "dotenv": "16.4.5",
    "ioredis": "5.4.1",
    "twilio": "5.13.1"
  },
  "devDependencies": {
    "@types/node": "20.12.7",
    "tsx": "4.7.2",
    "typescript": "5.4.5"
  }
}
```

Create `tsconfig.json` to configure strict type-safety checks:

```json
{
  "compilerOptions": {
    "target": "es2022",
    "module": "commonjs",
    "moduleResolution": "node",
    "lib": ["es2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "resolveJsonModule": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

---

### Step 2: Define Domain Types

Define the structures representing our alerting payloads and rate limiting boundaries.

Create `src/types.ts`:

```typescript
export type AlertSeverity = 'INFO' | 'WARNING' | 'CRITICAL';

export interface AlertEvent {
  id: string;          // Unique event UUID
  source: string;      // Generating service (e.g. 'auth-service')
  severity: AlertSeverity;
  message: string;     // Raw error details
  timestamp: number;   // Unix epoch timestamp in milliseconds
  fingerprint: string; // Signature used to partition rate-limiting limits
}

export interface RateLimitConfig {
  windowSeconds: number; // Rolling window duration
  maxRequests: number;   // Maximum allowed executions within the window
}

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  ttlRemaining: number; // Time remaining (seconds) until the rate-limit block expires
}
```

---

### Step 3: Implement Environment Validation & Mock Detection

The configuration layer validates required variables and implements **holistic mock detection**. It checks the structure of your credentials (ensuring the Account SID matches Twilio's standard length, the token is 32 hex digits, and the sender is a valid E.164 phone number). If validation fails, it defaults to mock mode, preventing cryptic connection or authentication crashes.

Create `src/config.ts`:

```typescript
import dotenv from 'dotenv';
import path from 'path';

// Force absolute path resolution to guarantee config loading regardless of run directories
dotenv.config({ path: path.resolve(__dirname, '../.env') });

function getEnvOrThrow(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `FATAL: Required environment variable '${name}' is not set.\n` +
      `  Copy .env.example to .env and fill in your values:\n` +
      `  cp .env.example .env`
    );
  }
  return value;
}

const rawAccountSid = getEnvOrThrow('TWILIO_ACCOUNT_SID');
const rawAuthToken = getEnvOrThrow('TWILIO_AUTH_TOKEN');
const rawPhoneNumber = getEnvOrThrow('TWILIO_PHONE_NUMBER');
const rawRecipient = getEnvOrThrow('ALERT_RECIPIENT_PHONE_NUMBER');

/**
 * Holistic mock detection: ALL three Twilio credentials must look real.
 * - Account SID must start with "AC" and be 34 chars (Twilio standard format)
 * - Auth Token must be 32 hex characters
 * - Phone Number must be E.164 format (+ followed by digits only)
 *
 * If ANY credential fails validation, we default to mock mode. This prevents
 * a half-configured .env from attempting live API calls that crash with
 * confusing Twilio error codes (21211, 20003, etc).
 */
function detectMockMode(): { isMock: boolean; reason: string } {
  const sidValid = /^AC[0-9a-f]{32}$/i.test(rawAccountSid);
  if (!sidValid) {
    return { isMock: true, reason: `TWILIO_ACCOUNT_SID is not a valid 34-char SID (got "${rawAccountSid.substring(0, 6)}...")` };
  }

  const tokenValid = /^[0-9a-f]{32}$/i.test(rawAuthToken);
  if (!tokenValid) {
    return { isMock: true, reason: 'TWILIO_AUTH_TOKEN is not a valid 32-char hex token' };
  }

  const phoneValid = /^\+[1-9]\d{1,14}$/.test(rawPhoneNumber);
  if (!phoneValid) {
    return { isMock: true, reason: `TWILIO_PHONE_NUMBER is not valid E.164 format (got "${rawPhoneNumber}")` };
  }

  return { isMock: false, reason: 'All Twilio credentials passed format validation' };
}

const mockDetection = detectMockMode();

// Validate recipient phone format and warn at import time
const recipientValid = /^\+[1-9]\d{1,14}$/.test(rawRecipient);
if (!recipientValid) {
  console.warn(
    `\x1b[33m[CONFIG WARNING]\x1b[0m ALERT_RECIPIENT_PHONE_NUMBER "${rawRecipient}" ` +
    `does not look like a valid E.164 phone number. SMS delivery will fail in live mode.`
  );
}

export const config = {
  twilio: {
    accountSid: rawAccountSid,
    authToken: rawAuthToken,
    phoneNumber: rawPhoneNumber,
    isMock: mockDetection.isMock,
    mockReason: mockDetection.reason,
  },
  alert: {
    recipientPhoneNumber: rawRecipient,
    windowSeconds: 60,
    maxRequests: 3,
  },
  redis: {
    url: getEnvOrThrow('REDIS_URL'),
    queueKey: 'alerting:queue',
    deadLetterKey: 'alerting:dead-letter',
  },
};
```

---

### Step 4: Write the Atomic Sliding-Window Lua Script

Our rate limiter utilizes a Redis Sorted Set (ZSET). The alert `fingerprint` is the key, the millisecond timestamp is the `score`, and a unique UUID is the `member`.

We execute the operations inside a single Lua script block:
1. **Prune Before Count:** The script must run `ZREMRANGEBYSCORE` *before* the cardinality count. If a client queries the limiter at `T=120,000ms` with a `60s` window, and the set contains entries at `T=50,000ms`, `T=80,000ms`, and `T=90,000ms`, the prune removes `T=50,000ms` (since `120000 - 60000 = 60000` is the lower boundary). The count drops to 2. If we counted before pruning, the request would be blocked.
2. **Conditional Expire:** If a request is allowed, we add it to the ZSET. Regardless of whether it's blocked or allowed, we call `PEXPIRE` to refresh the key's TTL. If we only set expiration on write, a crash between the write and the expiration could leave the key orphan in Redis forever, permanently blocking alerts for that fingerprint. Setting TTL always ensures the key self-cleans.
3. **Atomic Cooldown Retrieval:** If a request is blocked, we find the oldest entry's timestamp in the same execution. This eliminates a second round-trip `zrange` call, preventing race conditions where the ZSET changes between checks.

Create `src/limiter.ts`:

```typescript
import Redis from 'ioredis';
import { RateLimitConfig, RateLimitResult } from './types';

declare module 'ioredis' {
  interface Redis {
    checkRateLimit(
      key: string,
      now: string,
      windowMs: string,
      maxRequests: string,
      memberId: string
    ): Promise<[number, number, number]>;
  }
}

export class DistributedRateLimiter {
  private redis: Redis;

  constructor(redisClient: Redis) {
    this.redis = redisClient;

    this.redis.defineCommand('checkRateLimit', {
      numberOfKeys: 1,
      lua: `
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local windowMs = tonumber(ARGV[2])
        local maxRequests = tonumber(ARGV[3])
        local memberId = ARGV[4]

        -- 1. Prune logs older than current window boundary
        redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowMs)

        -- 2. Count remaining elements inside the window
        local currentCount = redis.call('ZCARD', key)

        local allowed = 0
        local oldestScore = 0
        if currentCount < maxRequests then
          -- 3a. Add request log
          redis.call('ZADD', key, now, memberId)
          currentCount = currentCount + 1
          allowed = 1
        else
          -- 3b. Read the oldest log's timestamp to compute cooldown atomically
          local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
          if oldest and #oldest == 2 then
            oldestScore = tonumber(oldest[2])
          end
        end

        -- 4. Always set TTL on the key to prevent orphaned keys on process crash
        redis.call('PEXPIRE', key, windowMs)

        return {allowed, currentCount, oldestScore}
      `,
    });
  }

  public async check(
    fingerprint: string,
    config: RateLimitConfig,
    alertId: string
  ): Promise<RateLimitResult> {
    const key = `rate_limit:${fingerprint}`;
    const now = Date.now();
    const windowMs = config.windowSeconds * 1000;

    // Single atomic round trip
    const [allowedRaw, currentCount, oldestScore] = await this.redis.checkRateLimit(
      key,
      now.toString(),
      windowMs.toString(),
      config.maxRequests.toString(),
      alertId
    );

    const allowed = allowedRaw === 1;

    let ttlRemaining = config.windowSeconds;
    if (!allowed && oldestScore > 0) {
      const timePassed = now - oldestScore;
      ttlRemaining = Math.max(0, Math.ceil((windowMs - timePassed) / 1000));
    }

    return {
      allowed,
      currentCount,
      ttlRemaining,
    };
  }
}
```

---

### Step 5: Implement the SMS Gateway Wrapper

The gateway handles Twilio communications. It translates common error codes (e.g. `20003` for auth issues, `21211` for phone formatting) into human-readable suggestions.

Create `src/twilio.ts`:

```typescript
import twilio from 'twilio';
import { config } from './config';

export class TwilioService {
  private client: ReturnType<typeof twilio> | null = null;

  constructor() {
    if (!config.twilio.isMock) {
      try {
        this.client = twilio(config.twilio.accountSid, config.twilio.authToken);
      } catch (err: any) {
        console.error(
          '\x1b[31m[TWILIO INIT ERROR]\x1b[0m Failed to initialize Twilio client:', err.message,
          '\n  Falling back to mock mode.'
        );
        config.twilio.isMock = true;
      }
    }
  }

  public async sendSms(to: string, body: string): Promise<string> {
    if (config.twilio.isMock || !this.client) {
      await new Promise((resolve) => setTimeout(resolve, 150));
      const mockSid = `SMmock_${Math.random().toString(36).substring(2, 17)}`;
      console.log(
        `\x1b[36m[MOCK TWILIO SMS]\x1b[0m Sent Alert Message to \x1b[33m${to}\x1b[0m.\n` +
        `  Body: "${body}"\n` +
        `  SID: ${mockSid}`
      );
      return mockSid;
    }

    try {
      const message = await this.client.messages.create({
        body,
        from: config.twilio.phoneNumber,
        to,
      });

      if (!message.sid) {
        throw new Error('Twilio response did not contain a Message SID.');
      }

      return message.sid;
    } catch (error: any) {
      const errorCode = error.code || 'UNKNOWN';
      const errorMessage = error.message || 'No details provided';

      let hint = '';
      if (errorCode === 20003) {
        hint = '\n  Hint: Authentication failed. Verify TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN in your .env file.';
      } else if (errorCode === 21211) {
        hint = '\n  Hint: Invalid "To" phone number. Ensure ALERT_RECIPIENT_PHONE_NUMBER is in E.164 format (+1234567890).';
      } else if (errorCode === 21608 || errorCode === 21610) {
        hint = '\n  Hint: The "From" number is not verified or not SMS-capable. Check TWILIO_PHONE_NUMBER in your .env file.';
      }

      console.error(
        `\x1b[31m[TWILIO ERROR]\x1b[0m Failed SMS delivery to ${to}. ` +
        `Code: ${errorCode} - ${errorMessage}${hint}`
      );

      throw new Error(`Twilio dispatch failure: [${errorCode}] ${errorMessage}`);
    }
  }
}
```

---

### Step 6: Create the Worker Queue Daemon

The queue consumer runs continuously in the background. 
* It instantiates the blocking connection with `maxRetriesPerRequest: null`, ensuring that `BLPOP` can remain blocked indefinitely without `ioredis` timing out and failing the connection.
* If Redis drops mid-`BLPOP`, the worker sleeps for 2 seconds to avoid CPU log-spam while `ioredis` automatically handles the TCP reconnection in the background.
* It includes a **Dead-Letter Queue (DLQ)** handler inside the parsing check. Any corrupted or unparseable payloads are written to the `alerting:dead-letter` key, ensuring that debug data is never lost.

Create `src/worker.ts`:

```typescript
import Redis from 'ioredis';
import { config } from './config';
import { DistributedRateLimiter } from './limiter';
import { TwilioService } from './twilio';
import { AlertEvent } from './types';

class AlertWorker {
  private redisMain: Redis;
  private redisBlocking: Redis;
  private limiter: DistributedRateLimiter;
  private twilio: TwilioService;
  private isShuttingDown = false;

  constructor() {
    this.redisMain = new Redis(config.redis.url, {
      maxRetriesPerRequest: null, // Essential for BLPOP blocking
      enableReadyCheck: true,
    });
    this.redisBlocking = new Redis(config.redis.url, {
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
    });
    
    this.limiter = new DistributedRateLimiter(this.redisMain);
    this.twilio = new TwilioService();

    this.setupErrorHandlers();
    this.setupSignalHandlers();
  }

  private setupErrorHandlers() {
    const logConnectionError = (type: string, error: any) => {
      if (this.isShuttingDown) return;
      console.error(`\x1b[31m[REDIS ERROR - ${type}]\x1b[0m Connection error:`, error.message);
    };

    this.redisMain.on('error', (err) => logConnectionError('MAIN', err));
    this.redisBlocking.on('error', (err) => logConnectionError('BLOCKING', err));
  }

  private setupSignalHandlers() {
    const shutdown = async (signal: string) => {
      if (this.isShuttingDown) return;
      this.isShuttingDown = true;
      
      console.log(`\n\x1b[33m[SHUTDOWN]\x1b[0m Received ${signal}. Cleaning up connections...`);
      
      this.redisBlocking.disconnect();
      await this.redisMain.quit();
      
      console.log('\x1b[32m[SHUTDOWN]\x1b[0m Clean shutdown complete.');
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  public async start() {
    if (config.alert.recipientPhoneNumber === '+1234567890') {
      console.warn(
        '\x1b[33m[CONFIG WARNING]\x1b[0m ALERT_RECIPIENT_PHONE_NUMBER is set to the ' +
        'placeholder value "+1234567890". SMS will not be delivered in live mode.\n' +
        '  Update your .env file with a real phone number.\n'
      );
    }

    console.log('\x1b[32m[WORKER]\x1b[0m Alert Worker Daemon initialized.');
    console.log(`[WORKER] Monitoring queue: "${config.redis.queueKey}"`);
    console.log(`[WORKER] SMS Mode: ${config.twilio.isMock ? '\x1b[36mSIMULATED\x1b[0m' : '\x1b[32mLIVE\x1b[0m'}`);
    console.log(`[WORKER] Mode reason: ${config.twilio.mockReason}`);
    console.log(`[WORKER] Recipient: ${config.alert.recipientPhoneNumber}`);
    console.log(`[WORKER] Rate limit: Max ${config.alert.maxRequests} per ${config.alert.windowSeconds}s per fingerprint`);
    console.log(`[WORKER] Dead-letter queue: "${config.redis.deadLetterKey}"\n`);

    while (!this.isShuttingDown) {
      try {
        const result = await this.redisBlocking.blpop(config.redis.queueKey, 0);
        
        if (!result || this.isShuttingDown) continue;
        
        const [_, payload] = result;
        await this.processPayload(payload);
      } catch (error: any) {
        if (this.isShuttingDown) break;
        
        console.error('\x1b[31m[WORKER ERROR]\x1b[0m Error during polling loop:', error.message);
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  private async processPayload(payload: string) {
    let alert: AlertEvent;
    
    try {
      alert = JSON.parse(payload);
    } catch (err) {
      console.error(
        '\x1b[31m[DEAD LETTER]\x1b[0m Failed to parse alert payload. ' +
        'Moving to dead-letter queue:', payload.substring(0, 200)
      );
      try {
        await this.redisMain.rpush(config.redis.deadLetterKey, payload);
      } catch (dlqErr: any) {
        console.error('\x1b[31m[DEAD LETTER ERROR]\x1b[0m Could not write to dead-letter queue:', dlqErr.message);
      }
      return;
    }

    const start = Date.now();
    console.log(
      `\x1b[34m[PROCESSING]\x1b[0m Received alert [${alert.id}] ` +
      `Source: ${alert.source} | Severity: ${alert.severity} | Fingerprint: ${alert.fingerprint}`
    );

    try {
      const limitResult = await this.limiter.check(
        alert.fingerprint,
        { windowSeconds: config.alert.windowSeconds, maxRequests: config.alert.maxRequests },
        alert.id
      );

      if (!limitResult.allowed) {
        console.warn(
          `\x1b[33m[SUPPRESSED]\x1b[0m Alert [${alert.id}] throttled. ` +
          `Limit exceeded (${limitResult.currentCount}/${config.alert.maxRequests} in ${config.alert.windowSeconds}s). ` +
          `Cooldown remaining: ${limitResult.ttlRemaining}s`
        );
        return;
      }

      const smsBody = `Alert: [${alert.severity}] [${alert.source}] ${alert.message} (Ref: ${alert.id.substring(0, 8)})`;
      
      const messageSid = await this.twilio.sendSms(
        config.alert.recipientPhoneNumber,
        smsBody
      );

      const duration = Date.now() - start;
      console.log(
        `\x1b[32m[SUCCESS]\x1b[0m Alert [${alert.id}] processed. ` +
        `SMS SID: ${messageSid} | Limit status: ${limitResult.currentCount}/${config.alert.maxRequests} | Time: ${duration}ms`
      );
    } catch (err: any) {
      console.error(`\x1b[31m[PROCESSING FAILED]\x1b[0m Alert [${alert.id}] could not be completed:`, err.message);
    }
  }
}

const worker = new AlertWorker();
worker.start().catch((err) => {
  console.error('\x1b[31m[FATAL]\x1b[0m Worker failed to start:', err.message);
  process.exit(1);
});
```

---

### Step 7: Create the Alert Publisher Utility

Create a client wrapper that microservices use to queue events.

Create `src/publisher.ts`:

```typescript
import Redis from 'ioredis';
import crypto from 'crypto';
import { config } from './config';
import { AlertEvent, AlertSeverity } from './types';

export class AlertPublisher {
  private redis: Redis;

  constructor() {
    this.redis = new Redis(config.redis.url);
    this.redis.on('error', (err) => {
      console.error('\x1b[31m[PUBLISHER REDIS ERROR]\x1b[0m', err.message);
    });
  }

  public async publish(alert: {
    source: string;
    severity: AlertSeverity;
    message: string;
    fingerprint?: string;
  }): Promise<string> {
    const id = crypto.randomUUID();
    const timestamp = Date.now();
    
    const fingerprint = alert.fingerprint || 
      crypto.createHash('sha256')
        .update(`${alert.source}:${alert.severity}`)
        .digest('hex');

    const alertEvent: AlertEvent = {
      id,
      timestamp,
      fingerprint,
      source: alert.source,
      severity: alert.severity,
      message: alert.message,
    };

    const payload = JSON.stringify(alertEvent);
    await this.redis.rpush(config.redis.queueKey, payload);
    
    return id;
  }

  public async close() {
    await this.redis.quit();
  }
}

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    console.log(
      '\x1b[33m[USAGE]\x1b[0m npm run publisher <message> [severity] [source] [fingerprint]\n' +
      '  Default severity: CRITICAL\n' +
      '  Default source: auth-service\n' +
      '  Example: npm run publisher "Redis database connection timed out" CRITICAL backend-db\n'
    );
    process.exit(0);
  }

  const message = args[0];
  const severity = (args[1] || 'CRITICAL').toUpperCase() as AlertSeverity;
  const source = args[2] || 'auth-service';
  const fingerprint = args[3];

  const publisher = new AlertPublisher();
  try {
    const alertId = await publisher.publish({
      source,
      severity,
      message,
      fingerprint
    });

    console.log(
      `\x1b[32m[PUBLISHED]\x1b[0m Alert successfully queued.\n` +
      `  ID: ${alertId}\n` +
      `  Source: ${source}\n` +
      `  Severity: ${severity}\n` +
      `  Message: "${message}"`
    );
  } catch (error: any) {
    console.error('\x1b[31m[PUBLISH FAILED]\x1b[0m', error.message);
  } finally {
    await publisher.close();
  }
}

if (require.main === module) {
  main();
}
```

---

## Cold-Clone Verification

Follow these steps exactly to run and test the pipeline on a clean machine:

### 1. Start Infrastructure
Start the Redis server using Docker Compose. The configuration contains a health check to guarantee Redis is fully loaded:
```bash
docker compose up -d
```

### 2. Install Dependencies & Build
Install packages and compile:
```bash
npm install
npm run build
```

### 3. Setup Configuration
Copy the environment template:
```bash
cp .env.example .env
```
*(Keep the default values inside `.env` to execute in mock simulation mode.)*

### 4. Execute the Worker Daemon
Open **Terminal 1** and start the worker daemon:
```bash
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

### 5. Run the Simulation Burst
Open **Terminal 2** and trigger the test pipeline:
```bash
npm run test-pipeline
```

**Expected Output in Terminal 2:**
```text
[TEST-RUNNER] Connecting to Redis...
[TEST-RUNNER] Cleaned up existing rate limits and queues.

[TEST-RUNNER] Publishing Alert Burst: 5 identical database failures...
  -> Queued [Burst #1] ID: 278e3fa2-0052-42ba-aa58-c70571c99311
  ...
[TEST-RUNNER] Simulation complete.
```

**Expected Log in Terminal 1 (Worker):**
You will observe the worker process:
1. Dispatch SMS alerts for the first 3 identical database issues.
2. Intercept and block the 4th and 5th database alerts, printing the remaining cooldown duration.
3. Successfully process and dispatch the isolated `oom_warning` alert, demonstrating partition safety.

```text
[PROCESSING] Received alert [278e3fa2...] Source: database-cluster | Severity: CRITICAL | Fingerprint: db_connection_fail
[MOCK TWILIO SMS] Sent Alert Message to +1234567890.
  Body: "Alert: [CRITICAL] [database-cluster] Connection pool exhausted - active connections > 100 [Burst #1] (Ref: 278e3fa2)"
  SID: SMmock_r40fcrlc6tn
[SUCCESS] Alert [278e3fa2...] processed. SMS SID: SMmock_r40fcrlc6tn | Limit status: 1/3 | Time: 153ms

[PROCESSING] Received alert [6e14d64b...] Source: database-cluster | Severity: CRITICAL | Fingerprint: db_connection_fail
[SUCCESS] Alert [6e14d64b...] processed. SMS SID: SMmock_6i4aboqtvp3 | Limit status: 2/3

[PROCESSING] Received alert [81462b81...] Source: database-cluster | Severity: CRITICAL | Fingerprint: db_connection_fail
[SUCCESS] Alert [81462b81...] processed. SMS SID: SMmock_axpntu0prnu | Limit status: 3/3

[PROCESSING] Received alert [a199b846...] Source: database-cluster | Severity: CRITICAL | Fingerprint: db_connection_fail
[SUPPRESSED] Alert [a199b846...] throttled. Limit exceeded (3/3 in 60s). Cooldown remaining: 60s

[PROCESSING] Received alert [d8b6138a...] Source: database-cluster | Severity: CRITICAL | Fingerprint: db_connection_fail
[SUPPRESSED] Alert [d8b6138a...] throttled. Limit exceeded (3/3 in 60s). Cooldown remaining: 60s

[PROCESSING] Received alert [72fde672...] Source: web-server | Severity: WARNING | Fingerprint: oom_warning
[SUCCESS] Alert [72fde672...] processed. SMS SID: SMmock_l8xm477jjm | Limit status: 1/3 | Time: 151ms
```

---

## Production Considerations

To deploy this microservice to high-throughput environments, evaluate the following scaling paradigms:

### 1. Redis Streams and Consumer Groups
While Redis Lists (`BLPOP`) are fast, they are not strictly fault-tolerant. If a worker pops an alert and immediately encounters a hardware crash, the message is permanently lost. 
To guarantee **at-least-once** delivery, transition to **Redis Streams** combined with **Consumer Groups (`XREADGROUP`)**. Redis Streams track a **Pending Entries List (PEL)**. If a worker retrieves an alert but fails to acknowledge it (`XACK`) within a specific window, another worker can claim the alert and process it.

### 2. Hash Tags for Cluster Compatibility
In a sharded Redis Cluster, keys are distributed across slots. Evaluating a Lua script referencing multiple keys (like a rate-limiting key and a tracking log) will crash with a `CROSSSLOT` error if they route to different hosts.
Solve this by wrapping the sharded keys in **hash tags** `{...}`. For example, use `{rate_limit:db_failure}` and `{rate_limit:db_failure}:metadata`. Redis hashes only the bracketed section, forcing both keys onto the same cluster node.

### 3. Asynchronous SMS Delivery Reconciliation
The HTTP response from `twilio.messages.create` only confirms that Twilio accepted your request, not that the SMS arrived at the handset. 
Provide a `statusCallback` URL parameter in your Twilio API call to direct events to a public callback hook (e.g. on ECS or AWS Lambda). Track the events (`sent`, `delivered`, `failed`, or `undelivered`). If a delivery changes to `failed` due to carrier routing or invalid formatting, trigger fallback routing to alternative alerting systems like Slack or PagerDuty.

---

## Conclusion

We have engineered a robust, decoupled, and rate-limited alerting system in TypeScript. Rather than bombarding SMS gateways directly, microservices append events asynchronously to a Redis Queue. Dedicated workers process alerts sequentially, using an atomic Lua script to enforce sliding-window rate limits before invoking Twilio.

This architecture balances decoupling with execution speed. While using a Sliding Window log incurs a higher Redis memory footprint compared to Token Bucket limiters (since we store individual timestamp values), it provides precise, race-free cooldown tracking that is critical for real-time diagnostics. Moving forward, this pipeline can be extended by upgrading the transport layer to Redis Streams for durable message recovery, or by implementing fallback routes to alternative paging channels if Twilio status callbacks report delivery failures. 

By taking control of your alerting traffic, you can protect both your system's operational budget and your engineering team's sleep.