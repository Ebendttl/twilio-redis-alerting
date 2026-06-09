export type AlertSeverity = 'INFO' | 'WARNING' | 'CRITICAL';

export interface AlertEvent {
  id: string;
  source: string;
  severity: AlertSeverity;
  message: string;
  timestamp: number;
  fingerprint: string; // Key used for deduplication and rate limiting
}

export interface RateLimitConfig {
  windowSeconds: number; // Duration of sliding window
  maxRequests: number;   // Max requests allowed within the window
}

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  ttlRemaining: number; // Time in seconds until the oldest log in the window expires
}
