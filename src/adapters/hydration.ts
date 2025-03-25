import { hydration } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { type Observable, from } from 'rxjs';
import { catchError, filter, map } from 'rxjs/operators';
import { BaseChainAdapter } from './base';

/**
 * Adapter for the Hydration chain
 */
export class HydrationAdapter extends BaseChainAdapter {
  private client: PolkadotClient | null = null;
  private api: TypedApi<typeof hydration> | null = null;
  readonly name = 'Hydration';

  /**
   * The RPC endpoint for the Hydration chain
   */
  private readonly endpoints = ['wss://hydration.ibp.network'];

  /**
   * The token ID for DOT on Hydration
   */
  private readonly tokenId = 5;

  /**
   * Connect to the Hydration chain
   */
  async connect(): Promise<void> {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(hydration);
    } catch (error) {
      this.logError('Failed to connect to Hydration', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Hydration chain
   */
  async disconnect(): Promise<void> {
    await super.disconnect();

    // Clean up the Hydration client
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.api = null;
    }
  }

  /**
   * Watch an account's balance on Hydration
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes
   */
  watchBalance(accountId: SS58String): Observable<bigint> {
    if (!this.api) {
      throw new Error(`${this.name} client not connected`);
    }

    // Create an observable from the Hydration API
    const balanceObservable = from(
      this.api.query.Tokens.Accounts.watchValue(accountId, this.tokenId),
    ).pipe(
      filter((content): content is NonNullable<typeof content> => content !== undefined),
      map((account) => account.free),
      filter((balance) => balance > 0n),
      catchError((error) => {
        this.logError(`Error watching balance for ${accountId}`, error);
        throw error;
      }),
    );

    // Store the subscription for later cleanup
    const subscription = balanceObservable.subscribe({
      next: (balance) => this.balanceSubject.next(balance),
      error: (error) => this.logError(`Subscription error for ${accountId}`, error),
    });

    this.subscriptions.set(`${accountId}-balance`, subscription);

    // Return the subject as an observable
    return this.balanceSubject.asObservable();
  }
}
