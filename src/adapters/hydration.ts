import { hydration } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { EMPTY, Observable, from } from 'rxjs'; // Added EMPTY
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
   * The token ID for DOT on Hydration.
   */
  private readonly tokenId = 5;

  /**
   * Connect to the Hydration chain
   */
  connect(): void {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(hydration);
      console.log(`[${this.name}] Connected`);
    } catch (error) {
      this.logError('Failed to connect to Hydration', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Hydration chain
   */
  disconnect(): void {
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.api = null;
      console.log(`[${this.name}] Disconnected`);
    }
  }

  /**
   * Watch an account's balance on Hydration
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes for that specific account
   */
  watchBalance(accountId: SS58String): Observable<bigint> {
    if (!this.api) {
      console.error(`${this.name} client not connected when calling watchBalance`);
      return new Observable((subscriber) => {
        subscriber.error(new Error(`${this.name} client not connected`));
      });
    }

    // Create an observable directly from the Hydration API call
    const balanceObservable = from(
      this.api.query.Tokens.Accounts.watchValue(accountId, this.tokenId),
    ).pipe(
      // Tokens.Accouxnts returns the full account info or undefined if no account
      filter(
        (accountInfo): accountInfo is NonNullable<typeof accountInfo> => accountInfo !== undefined,
      ),
      map((accountInfo) => accountInfo.free), // Extract the free balance
      catchError((error) => {
        this.logError(`Error watching balance for ${accountId} on ${this.name}`, error);
        throw error; // Re-throw
        // return EMPTY;
      }),
    );

    return balanceObservable;
  }
}
