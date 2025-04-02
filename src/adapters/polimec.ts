import { XcmV3Junctions, polimec } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { EMPTY, Observable, from } from 'rxjs'; // Added EMPTY
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
      console.log(`[${this.name}] Connected`);
    } catch (error) {
      this.logError('Failed to connect to Polimec', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Polimec chain
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
   * Watch an account's balance on Polimec
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
    const balanceObservable = from(
      this.api.query.ForeignAssets.Account.watchValue(
        { parents: 1, interior: XcmV3Junctions.Here() },
        accountId,
      ),
    ).pipe(
      // ForeignAssets.Account returns the full account info or undefined if no account
      // Filter out undefined cases (account doesn't exist for this asset)
      filter(
        (accountInfo): accountInfo is NonNullable<typeof accountInfo> => accountInfo !== undefined,
      ),
      map((accountInfo) => accountInfo.balance), // Extract the balance
      catchError((error) => {
        this.logError(`Error watching balance for ${accountId} on ${this.name}`, error);
        throw error; // Re-throw
        // return EMPTY;
      }),
    );

    return balanceObservable;
  }
}
