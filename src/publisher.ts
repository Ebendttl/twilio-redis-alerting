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

  /**
   * Publishes an alert event to the centralized Redis queue
   * @param alert Details of the alert event
   * @returns Generated alert ID
   */
  public async publish(alert: {
    source: string;
    severity: AlertSeverity;
    message: string;
    fingerprint?: string;
  }): Promise<string> {
    const id = crypto.randomUUID();
    const timestamp = Date.now();
    
    // Auto-generate a stable fingerprint if not provided (based on source and severity)
    // Dynamic message details are excluded from fingerprint to group related incidents together
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
    
    // RPUSH appends the message to the tail of the list queue
    await this.redis.rpush(config.redis.queueKey, payload);
    
    return id;
  }

  public async close() {
    await this.redis.quit();
  }
}

// Standalone CLI runner
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
  const fingerprint = args[3]; // Optional manual fingerprint override

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

// Check if file is run directly (commonjs/ts-node fallback)
if (require.main === module) {
  main();
}
