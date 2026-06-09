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
    ): Promise<[number, number]>;
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

        -- Prune logs older than current window boundary
        redis.call('ZREMRANGEBYSCORE', key, '-inf', now - windowMs)

        -- Determine volume of remaining requests
        local currentCount = redis.call('ZCARD', key)

        local allowed = 0
        if currentCount < maxRequests then
          -- Add request log with score = current time, member = unique ID
          redis.call('ZADD', key, now, memberId)
          -- Set TTL to match the window duration to prevent key accumulation
          redis.call('PEXPIRE', key, windowMs)
          currentCount = currentCount + 1
          allowed = 1
        end

        return {allowed, currentCount}
      `,
    });
  }

  /**
   * Check if a specific alert fingerprint is within limits
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

    // Invoke the custom script command
    const [allowedRaw, currentCount] = await this.redis.checkRateLimit(
      key,
      now.toString(),
      windowMs.toString(),
      config.maxRequests.toString(),
      alertId
    );

    const allowed = allowedRaw === 1;

    // Find the remaining TTL of the oldest item in the set to know when rate-limits reset
    let ttlRemaining = config.windowSeconds;
    if (!allowed) {
      const oldestArray = await this.redis.zrange(key, 0, 0, 'WITHSCORES');
      if (oldestArray.length === 2) {
        const oldestTimestamp = parseInt(oldestArray[1], 10);
        const timePassed = now - oldestTimestamp;
        ttlRemaining = Math.max(0, Math.ceil((windowMs - timePassed) / 1000));
      }
    }

    return {
      allowed,
      currentCount,
      ttlRemaining,
    };
  }
}
