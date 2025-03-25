import type { SS58String } from 'polkadot-api';
import { type Observable, Subject, merge } from 'rxjs';
import { map, tap } from 'rxjs/operators';
import type { ChainAdapter } from '../adapters';
import { Balance } from '../models/Balance';

/**
 * Service that aggregates balances from multiple chains
 */
export class BalanceService {
  private balance = new Balance();
  private totalBalanceSubject = new Subject<bigint>();

  /**
   * Create a new BalanceService
   * @param adapters The chain adapters to use
   */
  constructor(private adapters: ChainAdapter[]) {}

  /**
   * Start tracking balances for an account across all chains
   * @param accountId The SS58 formatted account address
   * @returns An Observable of the total balance
   */
  watchTotalBalance(accountId: SS58String): Observable<bigint> {
    // Create an array of observables from each adapter
    const balanceObservables = this.adapters.map((adapter) => {
      return adapter.watchBalance(accountId).pipe(
        tap((balance) => {
          // Update the balance model with the new value
          this.balance.setBalance(adapter.name, balance);

          // Emit the new total balance
          this.totalBalanceSubject.next(this.balance.getTotal());
        }),
        // Map to the chain name and balance for debugging
        map((balance) => ({ chain: adapter.name, balance })),
      );
    });

    // Merge all observables into one
    merge(...balanceObservables).subscribe({
      error: (error) => {
        console.error('[BalanceService] Error watching balances:', error);
      },
    });

    // Return the total balance observable
    return this.totalBalanceSubject.asObservable();
  }

  /**
   * Get the current balance
   * @returns The current Balance object
   */
  getCurrentBalance(): Balance {
    return this.balance;
  }

  /**
   * Clean up resources
   */
  dispose(): void {
    this.totalBalanceSubject.complete();
  }
}
