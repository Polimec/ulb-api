export class Balance {
  private onPolkadot: bigint;
  private onPolimec: bigint;

  constructor(public polkadotbalance = 0n, public polimecBalance = 0n) {
    this.onPolkadot = 0n;
    this.onPolimec = 0n;
  }

  setPolkadot(value: bigint) {
    this.onPolkadot = value;
  }
  setPolimec(value: bigint) {
    this.onPolimec = value;
  }

  total() {
    return this.onPolimec + this.onPolkadot;
  }
}
