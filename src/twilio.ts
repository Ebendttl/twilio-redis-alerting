import twilio from 'twilio';
import { config } from './config';

export class TwilioService {
  private client: ReturnType<typeof twilio> | null = null;

  constructor() {
    if (!config.twilio.isMock) {
      try {
        this.client = twilio(config.twilio.accountSid, config.twilio.authToken);
      } catch (err: any) {
        console.error(
          '\x1b[31m[TWILIO INIT ERROR]\x1b[0m Failed to initialize Twilio client:', err.message,
          '\n  Falling back to mock mode.'
        );
        config.twilio.isMock = true;
      }
    }
  }

  /**
   * Dispatches SMS alert notification.
   * In mock mode, simulates a 150ms network roundtrip and returns a fake SID.
   * In live mode, calls Twilio's messages.create API with structured error handling.
   *
   * @param to Recipient phone number in E.164 format
   * @param body Text message body
   * @returns Twilio Message SID string (or mock SID in simulation mode)
   */
  public async sendSms(to: string, body: string): Promise<string> {
    if (config.twilio.isMock || !this.client) {
      // Simulate network latency and return mock SID
      await new Promise((resolve) => setTimeout(resolve, 150));
      const mockSid = `SMmock_${Math.random().toString(36).substring(2, 17)}`;
      console.log(
        `\x1b[36m[MOCK TWILIO SMS]\x1b[0m Sent Alert Message to \x1b[33m${to}\x1b[0m.\n` +
        `  Body: "${body}"\n` +
        `  SID: ${mockSid}`
      );
      return mockSid;
    }

    try {
      const message = await this.client.messages.create({
        body,
        from: config.twilio.phoneNumber,
        to,
      });

      if (!message.sid) {
        throw new Error('Twilio response did not contain a Message SID.');
      }

      return message.sid;
    } catch (error: any) {
      const errorCode = error.code || 'UNKNOWN';
      const errorMessage = error.message || 'No details provided';

      // Provide actionable guidance for common Twilio error codes
      let hint = '';
      if (errorCode === 20003) {
        hint = '\n  Hint: Authentication failed. Verify TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN in your .env file.';
      } else if (errorCode === 21211) {
        hint = '\n  Hint: Invalid "To" phone number. Ensure ALERT_RECIPIENT_PHONE_NUMBER is in E.164 format (+1234567890).';
      } else if (errorCode === 21608 || errorCode === 21610) {
        hint = '\n  Hint: The "From" number is not verified or not SMS-capable. Check TWILIO_PHONE_NUMBER in your .env file.';
      }

      console.error(
        `\x1b[31m[TWILIO ERROR]\x1b[0m Failed SMS delivery to ${to}. ` +
        `Code: ${errorCode} - ${errorMessage}${hint}`
      );

      throw new Error(`Twilio dispatch failure: [${errorCode}] ${errorMessage}`);
    }
  }
}
