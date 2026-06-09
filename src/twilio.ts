import twilio from 'twilio';
import { config } from './config';

export class TwilioService {
  private client: ReturnType<typeof twilio> | null = null;

  constructor() {
    if (!config.twilio.isMock) {
      try {
        this.client = twilio(config.twilio.accountSid, config.twilio.authToken);
      } catch (err: any) {
        console.error('Failed to initialize live Twilio client. Switching to mock mode.', err.message);
        config.twilio.isMock = true;
      }
    }
  }

  /**
   * Dispatches SMS alert notification
   * @param to Recipient phone number in E.164 format
   * @param body Text message body
   * @returns Twilio Message SID string
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
      // Robust error grouping for operational visibility
      const errorCode = error.code || 'UNKNOWN';
      const errorMessage = error.message || 'No details provided';
      
      console.error(
        `\x1b[31m[TWILIO ERROR]\x1b[0m Failed SMS delivery to ${to}. ` +
        `Code: ${errorCode} - Message: ${errorMessage}`
      );

      throw new Error(`Twilio dispatch failure: [${errorCode}] ${errorMessage}`);
    }
  }
}
