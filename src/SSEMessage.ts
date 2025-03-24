import { safeStringify } from "./utils";

export class SSEMessage {
  private static encoder: TextEncoder = new TextEncoder();

  constructor(
    public data: string | bigint,
    public event?: string,
    public id?: string,
    public retry?: number
  ) {}

  /**
   * Encodes the SSE message to the proper format for Server-Sent Events
   * @returns A string in SSE format
   */
  encode() {
    let message = "";

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

    // Handle data, which could be a Promise
    const resolvedData = safeStringify(this.data);

    // If data contains newlines, it needs to be split and prefixed with "data:" on each line
    const dataLines = resolvedData.split("\n");
    for (const line of dataLines) {
      message += `data: ${line}\n`;
    }

    // End the message with an extra newline
    message += "\n";

    return SSEMessage.encoder.encode(message);
  }
}
