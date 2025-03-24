export class Balance {
  private onPolkadot: bigint;
  private onPolimec: bigint;
  private onHydration: bigint;

  constructor(
    public polkadotbalance = 0n,
    public polimecBalance = 0n,
    public hydrationBalance = 0n
  ) {
    this.onPolkadot = 0n;
    this.onPolimec = 0n;
    this.onHydration = 0n;
  }

  setPolkadot(value: bigint) {
    this.onPolkadot = value;
  }
  setPolimec(value: bigint) {
    this.onPolimec = value;
  }

  setHydration(value: bigint) {
    this.onHydration = value;
  }

  total() {
    return this.onPolimec + this.onPolkadot + this.onHydration;
  }
}
