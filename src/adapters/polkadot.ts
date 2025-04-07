import { dot } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { Observable, catchError, from, map } from 'rxjs';
import { BaseChainAdapter } from './base';

/**
 * Adapter for the Polkadot chain
 */
export class PolkadotAdapter extends BaseChainAdapter {
  private client: PolkadotClient | null = null;
  private api: TypedApi<typeof dot> | null = null;
  readonly name = 'polkadot';

  /**
   * The RPC endpoints for the Polkadot chain
   */
  private readonly endpoints = ['wss://rpc.ibp.network/polkadot', 'wss://polkadot.dotters.network'];

  /**
   * Connect to the Polkadot chain
   */
  connect(): void {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(dot);
      console.log(`[${this.name}] Connected`);
    } catch (error) {
      this.logError('Failed to connect to Polkadot', error);
      throw error; // Re-throw to allow handling upstream
    }
  }

  /**
   * Disconnect from the Polkadot chain
   */
  disconnect(): void {
    // Clean up the Polkadot client
    if (this.client) {
      this.client.destroy();
      this.client = null;
      this.api = null;
      console.log(`[${this.name}] Disconnected`);
    }
  }

  /**
   * Watch an account's balance on Polkadot
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes for that specific account
   */
  watchBalance(accountId: SS58String): Observable<bigint> {
    if (!this.api) {
      // Instead of throwing, maybe return an Observable that errors or is empty?
      // Or ensure connect() is always called and succeeds before this.
      // For simplicity now, we keep the throw, assuming connect was successful.
      console.error(`${this.name} client not connected when calling watchBalance`);
      // Return an observable that immediately errors out
      return new Observable((subscriber) => {
        subscriber.error(new Error(`${this.name} client not connected`));
      });
      // OR: throw new Error(`${this.name} client not connected`);
    }

    // Create an observable directly from the Polkadot API call
    const balanceObservable = from(this.api.query.System.Account.watchValue(accountId)).pipe(
      map((accountInfo) => accountInfo.data.free),
      catchError((error) => {
        this.logError(`Error watching balance for ${accountId} on ${this.name}`, error);
        // Decide how to handle errors here. Re-throwing allows BalanceService to catch it.
        // Alternatively, return EMPTY to silently stop updates for this chain on error.
        // Let's re-throw for now.
        throw error; // Re-throw the error to be handled by the subscriber (BalanceService)
        // return EMPTY; // Alternative: Silently stop stream for this chain
      }),
    );

    // Directly return the observable stream
    return balanceObservable;
  }
}
