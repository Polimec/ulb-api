/**
 * Utility for safely stringifying values including bigints
 * @param data The data to stringify
 * @returns A JSON string
 */
export const safeStringify = (data: unknown): string =>
  JSON.stringify(data, (_key, value) => (typeof value === 'bigint' ? value.toString() : value));

/**
 * Server-Sent Event message
 */
export class SSEMessage {
  private static encoder: TextEncoder = new TextEncoder();

  /**
   * Create a new SSE message
   * @param data The data payload
   * @param event Optional event type
   * @param id Optional message ID
   * @param retry Optional retry interval in milliseconds
   */
  constructor(
    public data: unknown,
    public event?: string,
    public id?: string,
    public retry?: number,
  ) {}

  /**
   * Encodes the SSE message to the proper format for Server-Sent Events
   * @returns A Uint8Array in SSE format
   */
  encode(): Uint8Array {
    let message = '';

    // Add event field if present
    if (this.event) {
      message += `event: ${this.event}\n`;
    }

    // Add id field if present
    if (this.id) {
      message += `id: ${this.id}\n`;
    }

    // Add retry field if present
    if (this.retry) {
      message += `retry: ${this.retry}\n`;
    }

    // Handle data with safe stringify
    const resolvedData = safeStringify(this.data);

    // If data contains newlines, split and prefix each line with "data:"
    const dataLines = resolvedData.split('\n');
    for (const line of dataLines) {
      message += `data: ${line}\n`;
    }

    // End the message with an extra newline
    message += '\n';

    return SSEMessage.encoder.encode(message);
  }
}
