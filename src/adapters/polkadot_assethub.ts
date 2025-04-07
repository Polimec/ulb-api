import { pah } from '@polkadot-api/descriptors';
import { type PolkadotClient, type SS58String, type TypedApi, createClient } from 'polkadot-api';
import { withPolkadotSdkCompat } from 'polkadot-api/polkadot-sdk-compat';
import { getWsProvider } from 'polkadot-api/ws-provider/web';
import { Observable, catchError, from, map } from 'rxjs';
import { BaseChainAdapter } from './base';

/**
 * Adapter for the Polkadot Asset Hub chain
 */
export class PolkadotAssetHub extends BaseChainAdapter {
  private client: PolkadotClient | null = null;
  private api: TypedApi<typeof pah> | null = null;
  readonly name = 'polkadot_assethub';

  /**
   * The RPC endpoint for the Polkadot Asset Hub chain
   */
  private readonly endpoints = [
    'wss://sys.ibp.network/asset-hub-polkadot',
    'wss://asset-hub-polkadot.dotters.network',
  ];

  /**
   * Connect to the Polkadot Asset Hub chain
   */
  connect(): void {
    try {
      this.client = createClient(withPolkadotSdkCompat(getWsProvider(this.endpoints)));
      this.api = this.client.getTypedApi(pah);
      console.log(`[${this.name}] Connected`);
    } catch (error) {
      this.logError('Failed to connect to Polkadot Asset Hub', error);
      throw error;
    }
  }

  /**
   * Disconnect from the Polkadot Asset Hub chain
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
   * Watch an account's balance on Polkadot Asset Hub
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
    const balanceObservable = from(this.api.query.System.Account.watchValue(accountId)).pipe(
      map((accountInfo) => (accountInfo ? accountInfo.data.free : 0n)),
      catchError((error) => {
        this.logError(`Error watching balance for ${accountId} on ${this.name}`, error);
        throw error; // Re-throw the error to propagate it to the subscriber
      }),
    );

    return balanceObservable;
  }
}
