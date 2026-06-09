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
    // ioredis reconnects automatically by default (retryStrategy returns delay).
    // When Redis crashes mid-BLPOP, the blocking connection throws, the catch
    // block sleeps 2s, and on the next loop iteration BLPOP is called on the
    // same client instance—which will have already reconnected or will reconnect
    // transparently. We configure lazyConnect:false (default) so ioredis manages
    // the TCP lifecycle for us. No manual reconnection logic is needed.
    this.redisMain = new Redis(config.redis.url, {
      maxRetriesPerRequest: null,   // Required for blocking commands—prevents ioredis from timing out BLPOP
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
      // Suppress noise during intentional shutdown
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

      // Stop the blocking pop immediately by disconnecting the client
      this.redisBlocking.disconnect();

      // Allow current transactions to finalize, then close main connection
      await this.redisMain.quit();

      console.log('\x1b[32m[SHUTDOWN]\x1b[0m Clean shutdown complete.');
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  /**
   * Main polling loop using blocking pop to consume alerts
   */
  public async start() {
    // Startup validation: warn clearly if recipient phone is a placeholder
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
        // BLPOP blocks the connection until an item is pushed. 0 means block indefinitely.
        // maxRetriesPerRequest:null in the constructor prevents ioredis from aborting
        // this call after its default retry limit.
        const result = await this.redisBlocking.blpop(config.redis.queueKey, 0);

        if (!result || this.isShuttingDown) continue;

        const [_, payload] = result;
        await this.processPayload(payload);
      } catch (error: any) {
        // If connection is intentionally severed during shutdown, exit cleanly
        if (this.isShuttingDown) break;

        console.error('\x1b[31m[WORKER ERROR]\x1b[0m Error during polling loop:', error.message);
        // Fixed 2s backoff to avoid hot-looping during Redis outages.
        // ioredis will auto-reconnect in the background; this delay simply
        // prevents log spam while the reconnection handshake completes.
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  private async processPayload(payload: string) {
    let alert: AlertEvent;

    try {
      alert = JSON.parse(payload);
    } catch (err) {
      // Dead-letter queue: push unparseable payloads to a separate Redis list
      // so they can be inspected later. Never silently discard data.
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
      // Evaluate rate limits based on key fingerprint
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

      // Format alert message payload for SMS readability
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

// Start worker daemon
const worker = new AlertWorker();
worker.start().catch((err) => {
  console.error('\x1b[31m[FATAL]\x1b[0m Worker failed to start:', err.message);
  process.exit(1);
});
