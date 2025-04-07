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

  // Track which adapters have reported
  private adapterReportStatus: Record<string, boolean> = {};

  /**
   * Create a new BalanceService
   * @param adapters The chain adapters to use
   */
  constructor(private adapters: ChainAdapter[]) {
    // Initialize adapter report status
    for (const adapter of this.adapters) {
      this.adapterReportStatus[adapter.name] = false;
    }
  }

  /**
   * Check if all adapters have reported at least once
   * @returns Whether all adapters have reported
   */
  private allAdaptersReported(): boolean {
    return Object.values(this.adapterReportStatus).every((reported) => reported);
  }

  /**
   * Start tracking balances for an account across all chains
   * @param accountId The SS58 formatted account address
   * @returns An Observable of the total balance
   */
  watchTotalBalance(accountId: SS58String): Observable<bigint> {
    // Reset adapter report status
    for (const adapter of this.adapters) {
      this.adapterReportStatus[adapter.name] = false;
    }

    // Create an array of observables from each adapter
    const balanceObservables = this.adapters.map((adapter) => {
      return adapter.watchBalance(accountId).pipe(
        tap({
          next: (balance) => {
            // Update the balance model with the new value
            this.balance.setBalance(adapter.name, balance);

            // Mark this adapter as having reported
            this.adapterReportStatus[adapter.name] = true;

            // Only emit the new total balance if all adapters have reported at least once,
            // or if we've already started emitting (meaning all adapters have reported before)
            if (this.allAdaptersReported()) {
              this.totalBalanceSubject.next(this.balance.getTotal());
            }
          },
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
    // Reset adapter report status
    for (const adapter of this.adapters) {
      this.adapterReportStatus[adapter.name] = false;
    }

    // Create an array of observables from each adapter
    const balanceObservables = this.adapters.map((adapter) => {
      return adapter.watchBalance(accountId).pipe(
        tap({
          next: (balance) => {
            // Update the balance model with the new value
            this.balance.setBalance(adapter.name, balance);

            // Mark this adapter as having reported
            this.adapterReportStatus[adapter.name] = true;

            // Update total balance internally
            const newTotal = this.balance.getTotal();

            // Only emit updates if all adapters have reported at least once
            if (this.allAdaptersReported()) {
              // Emit the new total balance
              this.totalBalanceSubject.next(newTotal);

              // Emit detailed balance information
              this.detailedBalanceSubject.next(this.balance.getDetailedBalance());
            }
          },
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
