import type { SS58String } from 'polkadot-api';
import { type Observable, Subject, map, merge, tap } from 'rxjs';
import type { ChainAdapter } from '../adapters';
import { Balance } from '../models/Balance';

/**
 * Balance update information
 */
export interface BalanceUpdate {
  total: string;
  chains: Record<string, string>;
}

/**
 * Service that aggregates balances from multiple chains
 */
export class BalanceService {
  private balance = new Balance();
  private totalBalanceSubject = new Subject<bigint>();
  private detailedBalanceSubject = new Subject<BalanceUpdate>();

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
   * Start tracking detailed balance information for an account across all chains
   * @param accountId The SS58 formatted account address
   * @returns An Observable of detailed balance information
   */
  watchDetailedBalance(accountId: SS58String): Observable<BalanceUpdate> {
    // Create an array of observables from each adapter
    const balanceObservables = this.adapters.map((adapter) => {
      return adapter.watchBalance(accountId).pipe(
        tap((balance) => {
          // Update the balance model with the new value
          this.balance.setBalance(adapter.name, balance);

          // Emit the new total balance
          this.totalBalanceSubject.next(this.balance.getTotal());

          // Emit detailed balance information
          this.detailedBalanceSubject.next(this.balance.getDetailedBalance());
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

    // Return the detailed balance observable
    return this.detailedBalanceSubject.asObservable();
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
    this.detailedBalanceSubject.complete();
  }
}
