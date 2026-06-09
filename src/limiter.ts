import Redis from 'ioredis';
import { RateLimitConfig, RateLimitResult } from './types';

// Extend ioredis client definition to support type-safe execution of our custom Lua command
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

    // Register our custom command with ioredis. It handles EVALSHA and caching under the hood.
    this.redis.defineCommand('checkRateLimit', {
      numberOfKeys: 1,
      lua: `
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local windowMs = tonumber(ARGV[2])
        local maxRequests = tonumber(ARGV[3])
        local memberId = ARGV[4]

        -- Step 1: Prune logs older than current window boundary.
        -- This MUST happen before the cardinality check. Consider a 60s window
        -- at T=120000ms with entries at T=50000, T=80000, T=90000. The prune
        -- removes T=50000 (older than 120000 - 60000 = 60000), leaving 2 entries.
        -- Without pruning first, ZCARD would return 3, potentially blocking a
        -- request that should be allowed.
        redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowMs)

        -- Step 2: Count entries remaining in the current window
        local currentCount = redis.call('ZCARD', key)

        local allowed = 0
        local oldestScore = 0
        if currentCount < maxRequests then
          -- Step 3a: Request is within limits. Record it.
          redis.call('ZADD', key, now, memberId)
          currentCount = currentCount + 1
          allowed = 1
        else
          -- Step 3b: Request is denied. Retrieve the oldest entry's timestamp
          -- so we can compute cooldown in this same atomic context.
          local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
          if oldest and #oldest == 2 then
            oldestScore = tonumber(oldest[2])
          end
        end

        -- Step 4: Always set TTL on the key to prevent orphaned keys.
        -- If the process crashed between ZADD and a separate PEXPIRE, the key
        -- would persist forever, permanently rate-limiting that fingerprint.
        -- By always setting TTL (even on denied requests, where the key already
        -- exists), we guarantee the key self-cleans within one window period.
        -- This is safe: the TTL is always >= the time until the oldest entry
        -- expires from the window, so no valid entry is prematurely evicted.
        redis.call('PEXPIRE', key, windowMs)

        return {allowed, currentCount, oldestScore}
      `,
    });
  }

  /**
   * Check if a specific alert fingerprint is within limits.
   *
   * The entire check—prune, count, conditional insert, TTL refresh, and
   * oldest-entry lookup—executes inside a single Lua script. Redis runs
   * Lua scripts atomically (single-threaded), so there is no race window
   * between reading the ZSET state and acting on it, even with multiple
   * concurrent worker instances.
   *
   * @param fingerprint Unique identifier of the alert type (e.g. database_down)
   * @param config Configuration for the rate limit
   * @param alertId Unique alert identifier to act as member in the sorted set
   */
  public async check(
    fingerprint: string,
    config: RateLimitConfig,
    alertId: string
  ): Promise<RateLimitResult> {
    const key = `rate_limit:${fingerprint}`;
    const now = Date.now();
    const windowMs = config.windowSeconds * 1000;

    // Single atomic round trip: prune + count + conditional insert + TTL + oldest lookup
    const [allowedRaw, currentCount, oldestScore] = await this.redis.checkRateLimit(
      key,
      now.toString(),
      windowMs.toString(),
      config.maxRequests.toString(),
      alertId
    );

    const allowed = allowedRaw === 1;

    // Compute cooldown from the oldest entry score returned by the Lua script.
    // No second round trip needed—this was read atomically inside the script.
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
