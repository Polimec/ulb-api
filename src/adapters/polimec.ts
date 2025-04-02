import { XcmV3Junctions, polimec } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { type Observable, from } from 'rxjs';
import { catchError, filter, map } from 'rxjs/operators';
import { BaseChainAdapter } from './base';

/**
 * Adapter for the Polimec chain
 */
export class PolimecAdapter extends BaseChainAdapter {
  private client: PolkadotClient | null = null;
  private api: TypedApi<typeof polimec> | null = null;
  readonly name = 'Polimec';

  /**
   * The RPC endpoint for the Polimec chain
   */
  private readonly endpoints = ['wss://rpc.polimec.org'];

  /**
   * Connect to the Polimec chain
   */
  connect(): void {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(polimec);
    } catch (error) {
      this.logError('Failed to connect to Polimec', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Polimec chain
   */
  disconnect(): void {
    super.disconnect();

    // Clean up the Polimec client
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.api = null;
    }
  }

  /**
   * Watch an account's balance on Polimec
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes
   */
  watchBalance(accountId: SS58String): Observable<bigint> {
    if (!this.api) {
      throw new Error(`${this.name} client not connected`);
    }

    // Create an observable from the Polimec API
    const balanceObservable = from(
      this.api.query.ForeignAssets.Account.watchValue(
        { parents: 1, interior: XcmV3Junctions.Here() },
        accountId,
      ),
    ).pipe(
      filter((content): content is NonNullable<typeof content> => content !== undefined),
      map((account) => account.balance),
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
