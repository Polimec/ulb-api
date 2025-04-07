import { DurableObject } from 'cloudflare:workers';
import type { SS58String } from 'polkadot-api';
import { type Observable, shareReplay } from 'rxjs';
import {
  type ChainAdapter,
  HydrationAdapter,
  PolimecAdapter,
  PolkadotAdapter,
  PolkadotAssetHub,
} from '../adapters';
import { BalanceService, type BalanceUpdate, StreamService } from '../services';

/**
 * Durable Object that manages chain connections and streaming
 * for a specific client connection
 */
export class Listener extends DurableObject<Env> {
  // Adapters for different chains
  private adapters: ChainAdapter[];
  private balanceService: BalanceService;
  private streamService: StreamService;
  private initialized = false;

  // Store the shared observable
  private sharedBalanceObservable: Observable<BalanceUpdate> | null = null;
  // Store the account ID this DO instance is managing
  private accountId: SS58String | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Initialize adapters for different chains
    this.adapters = [
      new PolkadotAdapter(),
      new PolimecAdapter(),
      new HydrationAdapter(),
      new PolkadotAssetHub(),
    ];

    this.balanceService = new BalanceService(this.adapters);

    this.streamService = new StreamService({
      heartbeatInterval: 30_000,
    });

    // Set up cleanup on alarm (or inactivity if needed)
    // Consider using ctx.storage.setAlarm(..., { allowConcurrency: true })
    // and ctx.storage.getAlarm() to manage inactivity cleanup more robustly
    // if the DO should shut down when no clients are connected.
    // For now, a simple 24h alarm.
    ctx.storage.setAlarm(Date.now() + 24 * 60 * 60 * 1000); // 24 hours
  }

  /**
   * Initialize chain connections
   */
  private async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    try {
      await Promise.all(this.adapters.map((adapter) => adapter.connect()));
      this.initialized = true;
      console.log('[Listener DO] Initialized adapters for account scope.');
    } catch (error) {
      console.error('[Listener DO] Failed to initialize chain connections:', error);
      // Potentially destroy self or signal error state if initialization fails critically
      throw error;
    }
  }

  /**
   * RPC method to subscribe to balance updates for an account
   * @param accountId The SS58 formatted account address
   * @returns A ReadableStream for SSE
   */
  async subscribe(accountId: SS58String): Promise<ReadableStream> {
    // Ensure adapters connections are initialized
    await this.initialize();

    // Store the accountId if this is the first subscription for this DO instance
    if (!this.accountId) {
      this.accountId = accountId;
      console.log(`[Listener DO] First subscription for account: ${accountId}`);
    } else if (this.accountId !== accountId) {
      // This scenario shouldn't happen if idFromName is used correctly,
      // but good to handle defensively.
      console.error(
        `[Listener DO] Mismatched account ID: Expected ${this.accountId}, got ${accountId}`,
      );
      throw new Error('Internal error: Account ID mismatch in Durable Object instance.');
    }

    // Check if the shared observable is already created
    if (!this.sharedBalanceObservable) {
      console.log(`[Listener DO] Creating shared balance observable for ${this.accountId}`);
      // Start watching the account's detailed balance across all chains
      this.sharedBalanceObservable = this.balanceService.watchDetailedBalance(this.accountId).pipe(
        // Share the subscription and replay the last emitted value (the latest balance)
        // refCount: false means the underlying subscription stays active even if all clients disconnect,
        // until the DO is cleaned up. Set to true if you want it to stop when clients leave.
        shareReplay({ bufferSize: 1, refCount: false }),
      );
    }

    // Create an SSE stream using the *shared* balance observable
    console.log(`[Listener DO] Creating SSE stream for client, account: ${this.accountId}`);
    return this.streamService.createStream(this.accountId, this.sharedBalanceObservable);
  }

  /**
   * Clean up resources when the Durable Object is about to be destroyed
   * This is triggered by the alarm.
   */
  async alarm(): Promise<void> {
    console.log(`[Listener DO] Alarm triggered. Cleaning up for account: ${this.accountId}`);
    await this.cleanup();
  }

  /**
   * Clean up all resources
   */
  private async cleanup(): Promise<void> {
    if (!this.initialized) {
      return;
    }
    console.log(`[Listener DO] Starting cleanup for account: ${this.accountId}`);

    // Dispose of services first (stops intervals, completes subjects)
    this.streamService.dispose();
    this.balanceService.dispose();

    // Explicitly nullify the shared observable to release references
    this.sharedBalanceObservable = null;

    // Disconnect adapters
    await Promise.allSettled(this.adapters.map((adapter) => adapter.disconnect()));

    this.initialized = false;
    this.accountId = null; // Reset accountId
    console.log('[Listener DO] Cleanup complete.');
  }
}
