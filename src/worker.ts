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
    // Separate connections: redisBlocking will be locked by BLPOP, redisMain handles standard commands
    this.redisMain = new Redis(config.redis.url);
    this.redisBlocking = new Redis(config.redis.url);
    
    this.limiter = new DistributedRateLimiter(this.redisMain);
    this.twilio = new TwilioService();

    this.setupErrorHandlers();
    this.setupSignalHandlers();
  }

  private setupErrorHandlers() {
    const logConnectionError = (type: string, error: any) => {
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
    console.log('\x1b[32m[WORKER]\x1b[0m Alert Worker Daemon initialized.');
    console.log(`[WORKER] Monitoring queue: "${config.redis.queueKey}"`);
    console.log(`[WORKER] SMS Mode: ${config.twilio.isMock ? 'SIMULATED' : 'LIVE'}`);
    console.log(`[WORKER] Recipient: ${config.alert.recipientPhoneNumber}`);
    console.log(`[WORKER] Rate limit rules: Max ${config.alert.maxRequests} per ${config.alert.windowSeconds}s per fingerprint.\n`);

    while (!this.isShuttingDown) {
      try {
        // BLPOP blocks the connection until an item is pushed. 0 means block indefinitely.
        // Returns [key, value] or null on timeout/disconnect.
        const result = await this.redisBlocking.blpop(config.redis.queueKey, 0);
        
        if (!result || this.isShuttingDown) continue;
        
        const [_, payload] = result;
        await this.processPayload(payload);
      } catch (error: any) {
        // If connection is intentionally severed during shutdown, exit cleanly
        if (this.isShuttingDown) break;
        
        console.error('\x1b[31m[WORKER ERROR]\x1b[0m Error during polling loop:', error.message);
        // Exponential backoff to avoid hot-looping during database outages
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  private async processPayload(payload: string) {
    let alert: AlertEvent;
    
    try {
      alert = JSON.parse(payload);
    } catch (err) {
      console.error('\x1b[31m[WORKER ERROR]\x1b[0m Failed to parse alert payload JSON:', payload);
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
worker.start();
