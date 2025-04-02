import type { SS58String } from 'polkadot-api';
import { type Observable, Subject, type Subscription } from 'rxjs';

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
   * @returns An Observable of balance changes
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
  protected balanceSubject = new Subject<bigint>();
  protected subscriptions = new Map<string, Subscription>();

  /**
   * The chain's name
   */
  abstract readonly name: string;

  /**
   * Connect to the chain
   */
  abstract connect(): void;

  /**
   * Disconnect from the chain and clean up subscriptions
   */
  disconnect(): void {
    // Unsubscribe from all active subscriptions
    for (const subscription of this.subscriptions.values()) {
      subscription.unsubscribe();
    }
    this.subscriptions.clear();

    // Complete the balance subject
    this.balanceSubject.complete();
  }

  /**
   * Watch an account's balance
   * @param accountId The SS58 formatted account address
   * @returns An Observable of balance changes
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
