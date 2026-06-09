import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from .env file
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
    // Sliding window rules: Allow maximum 3 alerts per 60 seconds per fingerprint
    windowSeconds: 60,
    maxRequests: 3,
  },
  redis: {
    url: getEnvOrThrow('REDIS_URL'),
    queueKey: 'alerting:queue',
    deadLetterKey: 'alerting:dead-letter',
  },
};
