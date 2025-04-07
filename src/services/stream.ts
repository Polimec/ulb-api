import { generate } from '@std/uuid/unstable-v7';
import type { SS58String } from 'polkadot-api';
import { type Observable, fromEvent, interval, takeUntil } from 'rxjs';
import { SSEMessage } from '../models';
import type { BalanceUpdate } from './balance';

/**
 * Configuration for the StreamService
 */
export interface StreamServiceConfig {
  /**
   * Interval for sending heartbeat messages in milliseconds
   * @default 30000 (30 seconds)
   */
  heartbeatInterval?: number;
}

/**
 * Service for handling Server-Sent Events streams
 */
export class StreamService {
  private readonly heartbeatInterval: number;
  private readonly abortController = new AbortController();

  /**
   * Create a new StreamService
   * @param config Optional configuration
   */
  constructor(config?: StreamServiceConfig) {
    this.heartbeatInterval = config?.heartbeatInterval ?? 30_000;
  }

  /**
   * Create a new SSE stream for an account
   * @param accountId The SS58 formatted account address
   * @param balanceObservable An Observable of balance updates
   * @returns A ReadableStream for SSE
   */
  createStream(
    accountId: SS58String,
    balanceObservable: Observable<BalanceUpdate>,
  ): ReadableStream {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();

    // Send initial account ID message
    const initialMessage = new SSEMessage(accountId, 'accountId', generate());
    writer.write(initialMessage.encode());

    // Set up balance updates
    balanceObservable.subscribe({
      next: (balanceUpdate) => {
        try {
          // Send detailed balance information with the same event name for backward compatibility
          const message = new SSEMessage(balanceUpdate, 'DOT', generate());
          writer.write(message.encode());
        } catch (error) {
          console.error('[StreamService] Failed to write balance update:', error);
        }
      },
      error: (error) => {
        console.error('[StreamService] Balance observable error:', error);
        // Try to close the stream gracefully
        this.closeStream(writer);
      },
      complete: () => {
        console.log('[StreamService] Balance observable completed');
        this.closeStream(writer);
      },
    });

    // Set up heartbeat
    interval(this.heartbeatInterval)
      .pipe(takeUntil(fromEvent(this.abortController.signal, 'abort')))
      .subscribe({
        next: () => {
          try {
            const message = new SSEMessage(new Date().toISOString(), 'heartbeat', generate());
            writer.write(message.encode());
          } catch (error) {
            console.error('[StreamService] Failed to send heartbeat:', error);
            this.abortController.abort();
          }
        },
        error: (error) => {
          console.error('[StreamService] Heartbeat error:', error);
          this.closeStream(writer);
        },
      });

    return readable;
  }

  /**
   * Close the stream and clean up resources
   * @param writer The writer to close
   */
  private async closeStream(writer: WritableStreamDefaultWriter): Promise<void> {
    // Abort any pending operations
    this.abortController.abort();

    // Close the writer
    try {
      await writer.close();
    } catch (error) {
      console.error('[StreamService] Error closing writer:', error);
    }
  }

  /**
   * Clean up resources
   */
  dispose(): void {
    this.abortController.abort();
  }
}
