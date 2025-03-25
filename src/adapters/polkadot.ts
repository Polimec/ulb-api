import { dot } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { type Observable, from } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { BaseChainAdapter } from './base';

/**
 * Adapter for the Polkadot chain
 */
export class PolkadotAdapter extends BaseChainAdapter {
  private client: PolkadotClient | null = null;
  private api: TypedApi<typeof dot> | null = null;
  readonly name = 'Polkadot';

  /**
   * The RPC endpoints for the Polkadot chain
   */
  private readonly endpoints = ['wss://rpc.ibp.network/polkadot', 'wss://polkadot.dotters.network'];

  /**
   * Connect to the Polkadot chain
   */
  async connect(): Promise<void> {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(dot);
    } catch (error) {
      this.logError('Failed to connect to Polkadot', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Polkadot chain
   */
  async disconnect(): Promise<void> {
    await super.disconnect();

    // Clean up the Polkadot client
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.api = null;
    }
  }

  /**
   * Watch an account's balance on Polkadot
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes
   */
  watchBalance(accountId: SS58String): Observable<bigint> {
    if (!this.api) {
      throw new Error(`${this.name} client not connected`);
    }

    // Create an observable from the Polkadot API
    const balanceObservable = from(this.api.query.System.Account.watchValue(accountId)).pipe(
      map((account) => {
        // Only emit if there's a positive balance
        if (account.data.free > 0n) {
          return account.data.free;
        }
        return 0n;
      }),
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
