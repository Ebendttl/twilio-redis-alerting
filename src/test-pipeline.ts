import Redis from 'ioredis';
import { config } from './config';
import { AlertPublisher } from './publisher';

async function runTestSimulation() {
  console.log('\x1b[35m[TEST-RUNNER]\x1b[0m Connecting to Redis...');
  const redis = new Redis(config.redis.url, {
    maxRetriesPerRequest: 3,
    retryStrategy(times: number) {
      if (times > 3) return null;
      return Math.min(times * 200, 1000);
    },
  });

  redis.on('error', () => {});

  try {
    await redis.ping();
  } catch (err: any) {
    console.error(
      '\x1b[31m[TEST-RUNNER ERROR]\x1b[0m Cannot connect to Redis at ' +
      `${config.redis.url}.\n` +
      '  Ensure Redis is running:\n' +
      '    docker compose up -d\n' +
      `  Error: ${err.message}`
    );
    redis.disconnect();
    process.exit(1);
  }

  try {
    const keys = await redis.keys('rate_limit:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }
    await redis.del(config.redis.queueKey);
    console.log('\x1b[35m[TEST-RUNNER]\x1b[0m Cleaned up existing rate limits and queues.');

    const clientList = await redis.client('LIST') as string;
    const hasBlockedClient = clientList.split('\n').some(
      (line) => line.includes('cmd=blpop') || line.includes('cmd=BLPOP')
    );

    if (!hasBlockedClient) {
      console.warn(
        '\x1b[33m[TEST-RUNNER WARNING]\x1b[0m No active worker detected on the queue.\n' +
        '  Alerts will be queued but not processed until you start the worker:\n' +
        '    npm run worker\n'
      );
    }

    const publisher = new AlertPublisher();

    console.log('\n\x1b[35m[TEST-RUNNER]\x1b[0m Publishing Alert Burst: 5 identical database failures...');

    const dbFingerprint = 'db_connection_fail';
    for (let i = 1; i <= 5; i++) {
      const alertId = await publisher.publish({
        source: 'database-cluster',
        severity: 'CRITICAL',
        message: `Connection pool exhausted - active connections > 100 [Burst #${i}]`,
        fingerprint: dbFingerprint,
      });
      console.log(`  -> Queued [Burst #${i}] ID: ${alertId}`);
    }

    console.log('\n\x1b[35m[TEST-RUNNER]\x1b[0m Publishing 1 alternative alert (OOM) with different fingerprint...');
    const oomAlertId = await publisher.publish({
      source: 'web-server',
      severity: 'WARNING',
      message: 'Node process memory consumption exceeded 85%',
      fingerprint: 'oom_warning',
    });
    console.log(`  -> Queued [OOM] ID: ${oomAlertId}`);

    await publisher.close();

    console.log('\n\x1b[32m[TEST-RUNNER] Simulation complete.\x1b[0m');
    console.log('Expected results in the worker terminal:');
    console.log('  1. Three database alerts dispatched (SMS sent).');
    console.log('  2. Two database alerts suppressed (rate-limited).');
    console.log('  3. One OOM alert dispatched (separate fingerprint, not rate-limited).');

  } catch (error: any) {
    console.error('\x1b[31m[TEST-RUNNER ERROR]\x1b[0m Simulation failed:', error.message);
  } finally {
    await redis.quit();
  }
}

runTestSimulation();
