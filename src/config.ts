import dotenv from 'dotenv';
import path from 'path';

// Load environment variables from .env file
dotenv.config({ path: path.resolve(__dirname, '../.env') });

function getEnvOrThrow(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`CRITICAL: Environment variable '${name}' is missing.`);
  }
  return value;
}

const rawAccountSid = getEnvOrThrow('TWILIO_ACCOUNT_SID');
const rawAuthToken = getEnvOrThrow('TWILIO_AUTH_TOKEN');

// Check if credentials are using the default Twilio placeholders
const isTwilioMock = 
  rawAccountSid.startsWith('ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') || 
  rawAuthToken === 'your_auth_token_here';

export const config = {
  twilio: {
    accountSid: rawAccountSid,
    authToken: rawAuthToken,
    phoneNumber: getEnvOrThrow('TWILIO_PHONE_NUMBER'),
    isMock: isTwilioMock,
  },
  alert: {
    recipientPhoneNumber: getEnvOrThrow('ALERT_RECIPIENT_PHONE_NUMBER'),
    // Sliding window rules: Allow maximum 3 alerts per 60 seconds per fingerprint
    windowSeconds: 60,
    maxRequests: 3,
  },
  redis: {
    url: getEnvOrThrow('REDIS_URL'),
    queueKey: 'alerting:queue',
  },
};
