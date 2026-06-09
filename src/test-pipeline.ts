import Redis from 'ioredis';
import { config } from './config';
import { AlertPublisher } from './publisher';

async function runTestSimulation() {
  console.log('\x1b[35m[TEST-RUNNER]\x1b[0m Connecting to Redis to reset pipeline state...');
  const redis = new Redis(config.redis.url);
  
  try {
    // Clean slate for test verification
    const keys = await redis.keys('rate_limit:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }
    await redis.del(config.redis.queueKey);
    console.log('\x1b[35m[TEST-RUNNER]\x1b[0m Cleaned up existing rate limits and queues.');

    const publisher = new AlertPublisher();

    console.log('\n\x1b[35m[TEST-RUNNER]\x1b[0m Publishing Alert Burst: 5 identical database failures...');
    
    // We send 5 database alerts. The first 3 should process; 4 and 5 must be throttled.
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
    // This should immediately succeed, proving isolation
    const oomAlertId = await publisher.publish({
      source: 'web-server',
      severity: 'WARNING',
      message: 'Node process memory consumption exceeded 85%',
      fingerprint: 'oom_warning',
    });
    console.log(`  -> Queued [OOM] ID: ${oomAlertId}`);

    await publisher.close();
    
    console.log('\n\x1b[32m[TEST-RUNNER] Simulation payload successfully queued.\x1b[0m');
    console.log('Verify results in your active worker console. You should observe:');
    console.log('  1. Three database alerts succeed.');
    console.log('  2. Two database alerts get suppressed/throttled.');
    console.log('  3. One OOM alert succeeds (rate-limiting is isolated per fingerprint).');
    
  } catch (error: any) {
    console.error('\x1b[31m[TEST-RUNNER ERROR]\x1b[0m Simulation run crashed:', error.message);
  } finally {
    await redis.quit();
  }
}

runTestSimulation();
