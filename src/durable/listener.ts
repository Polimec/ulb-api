import { DurableObject } from 'cloudflare:workers';
import type { SS58String } from 'polkadot-api';
import { HydrationAdapter, PolimecAdapter, PolkadotAdapter } from '../adapters';
import { BalanceService, StreamService } from '../services';

/**
 * Durable Object that manages chain connections and streaming
 * for a specific client connection
 */
export class Listener extends DurableObject<Env> {
  private polkadotAdapter = new PolkadotAdapter();
  private polimecAdapter = new PolimecAdapter();
  private hydrationAdapter = new HydrationAdapter();
  private balanceService: BalanceService;
  private streamService: StreamService;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Initialize services
    this.balanceService = new BalanceService([
      this.polkadotAdapter,
      this.polimecAdapter,
      this.hydrationAdapter,
    ]);

    this.streamService = new StreamService({
      heartbeatInterval: 30_000,
    });

    // Set up cleanup on alarm
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
      // Connect to all chains in parallel
      await Promise.all([
        this.polkadotAdapter.connect(),
        this.polimecAdapter.connect(),
        this.hydrationAdapter.connect(),
      ]);

      this.initialized = true;
    } catch (error) {
      console.error('[Listener] Failed to initialize chain connections:', error);
      throw error;
    }
  }

  /**
   * RPC method to subscribe to balance updates for an account
   * @param accountId The SS58 formatted account address
   * @returns A ReadableStream for SSE
   */
  async subscribe(accountId: SS58String): Promise<ReadableStream> {
    // Ensure connections are initialized
    await this.initialize();

    // Start watching the account's detailed balance across all chains
    const balanceObservable = this.balanceService.watchDetailedBalance(accountId);

    // Create an SSE stream with the detailed balance updates
    return this.streamService.createStream(accountId, balanceObservable);
  }

  /**
   * Clean up resources when the Durable Object is about to be destroyed
   */
  async alarm(): Promise<void> {
    await this.cleanup();
  }

  /**
   * Clean up all resources
   */
  private async cleanup(): Promise<void> {
    // Only clean up if we were initialized
    if (!this.initialized) {
      return;
    }

    // Dispose of services
    this.streamService.dispose();
    this.balanceService.dispose();

    // Disconnect from chains
    await Promise.allSettled([
      this.polkadotAdapter.disconnect(),
      this.polimecAdapter.disconnect(),
      this.hydrationAdapter.disconnect(),
    ]);

    this.initialized = false;
  }
}
