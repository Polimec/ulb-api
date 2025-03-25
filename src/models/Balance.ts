/**
 * Represents balances across multiple chains
 */
export class Balance {
  private balances: Map<string, bigint> = new Map();

  /**
   * Set a balance for a specific chain
   * @param chain The chain identifier
   * @param value The balance value
   */
  setBalance(chain: string, value: bigint): void {
    this.balances.set(chain, value);
  }

  /**
   * Get a balance for a specific chain
   * @param chain The chain identifier
   * @returns The balance value or 0n if not set
   */
  getBalance(chain: string): bigint {
    return this.balances.get(chain) || 0n;
  }

  /**
   * Get the total balance across all chains
   * @returns The sum of all balances
   */
  getTotal(): bigint {
    let total = 0n;
    for (const balance of this.balances.values()) {
      total += balance;
    }
    return total;
  }

  /**
   * Get all balances as an object
   * @returns An object mapping chain names to balance values
   */
  toObject(): Record<string, string> {
    const result: Record<string, string> = {};
    for (const [chain, balance] of this.balances.entries()) {
      result[chain] = balance.toString();
    }
    return result;
  }
}
