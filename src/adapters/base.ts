import type { SS58String } from 'polkadot-api';
import type { Observable } from 'rxjs';

/**
 * Base Chain Adapter interface that defines common functionality
 * for connecting to and observing blockchain data
 */
export interface ChainAdapter {
  /**
   * Connect to the chain
   */
  connect(): void;

  /**
   * Disconnect from the chain
   */
  disconnect(): void;

  /**
   * Watch an account's balance
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes for that specific account
   */
  watchBalance(accountId: SS58String): Observable<bigint>;

  /**
   * The chain's name
   */
  readonly name: string;
}

/**
 * Base abstract class for chain adapters that implements common functionality
 */
export abstract class BaseChainAdapter implements ChainAdapter {
  /**
   * The chain's name
   */
  abstract readonly name: string;

  /**
   * Connect to the chain
   */
  abstract connect(): void;

  /**
   * Disconnect from the chain.
   * Adapter-specific implementations should handle client cleanup here.
   */
  abstract disconnect(): void;

  /**
   * Watch an account's balance
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes for that specific account
   */
  abstract watchBalance(accountId: SS58String): Observable<bigint>;

  /**
   * Helper method to log errors consistently
   * @param message Error message
   * @param error The error object
   */
  protected logError(message: string, error: unknown): void {
    console.error(`[${this.name}] ${message}:`, error);
  }
}
