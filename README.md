# Unified Liquidity Balance API

## Project Overview

Unified Liquidity Balance API is a real-time balance tracking solution for the Polkadot ecosystem, providing developers with a simple way to monitor user balances across multiple parachains simultaneously. By leveraging Server-Sent Events (SSE) technology and Cloudflare's edge infrastructure, this API offers a lightweight, scalable, efficient way to stream balance updates directly to applications without frequent polling.

## The Problem

Developers building cross-chain applications in the Polkadot ecosystem face significant challenges:

1. **Fragmented Balance Monitoring**: Applications need to separately connect to and poll multiple parachains to track user balances.
2. **Dependency on Polkadot-Specific Libraries**: Integrating balance monitoring typically requires using specialized libraries (`@polkadot/api`, `polkadot-api`, `dedot`) for direct interaction with RPC nodes or light clients. While these libraries are powerful and **essential for achieving decentralization** (especially light-client-first ones), they present a steeper learning curve and integration hurdle for developers or companies primarily focused on standard web technologies, potentially discouraging adoption of the Polkadot stack.
3. **Complex Integration**: Each parachain has unique APIs and data structures, increasing integration complexity.

## Our Solution

CrossChain Balance Monitor API solves these problems by:

1. **Unified API Endpoint**: Providing a single, simple API endpoint using standard web protocols (**Server-Sent Events over HTTPS**) that monitors balances across Polkadot, Polkadot Asset Hub, Polimec, Hydration, and other connected parachains.
2. **Server-Sent Events**: Pushing **real-time** balance updates to clients only when they change, reducing bandwidth usage.
3. **Edge Computing**: Leveraging Cloudflare Workers and Durable Objects to maintain persistent connections at the edge, closer to users.
4. **Reactive Architecture**: Using RxJS to efficiently handle asynchronous data flows.

## Technical Implementation

### Architecture

This project is built using:

- **Cloudflare Workers**: Serverless edge computing platform
- **Durable Objects**: Maintaining stateful connections
- **Polkadot API**: Connecting to various Polkadot ecosystem chains
- **RxJS**: Managing reactive data streams
- **TypeScript**: Ensuring type safety and maintainability

The system is structured in a modular way:

```
src/
├── adapters/           # Chain-specific adapters
├── services/           # Core business logic
├── models/             # Data models
├── durable/            # Cloudflare Durable Objects logic
└── utils/              # Utility functions
```

### Key Components

1. **Chain Adapters**: Each chain has its own adapter that handles connection and data retrieval specific to that chain, allowing easy extension to new chains.
2. **Balance Service**: Aggregates balances from multiple chains, providing a unified view of a user's assets.
3. **Stream Service**: Handles SSE streaming, including proper connection management and heartbeats.
4. **Listener Durable Object**: Maintains persistent connections to multiple chains and efficiently streams updates to clients.

## Benefits to the Ecosystem

This API provides several key benefits to the Polkadot ecosystem:

1.  **Reduced Developer Friction**: Simplifies integration by offering a standard HTTP/SSE interface, **removing the need for deep Polkadot-specific library knowledge** and lowering the barrier for web developers.
2.  **Better User Experience**: Enables applications to show real-time balance updates without frequent refreshing.
3.  **Lower Resource Requirements**: Offloads the complex task of chain monitoring from client applications.
4.  **Ecosystem Growth**: Lowers the barrier for developing cross-chain applications.
5.  **Infrastructure Improvement**: Provides a critical piece of infrastructure for cross-chain DApps.

## Usage

Using the API is straightforward:

```javascript
// Example client-side code for handling the enhanced balance events

// Connect to the SSE endpoint with a Polkadot address
const eventSource = new EventSource(
  "https://polkadot-cloud-sse.polimec.workers.dev/1qnJN7FViy3HZaxZK9tGAA71zxHSBeUweirKqCaox4t8GT7"
);

// Listen for balance updates - handles both the old and new format
eventSource.addEventListener("DOT", (event) => {
  const data = JSON.parse(event.data);

  console.log(`Total DOT balance: ${data.total}`);

  // Log individual chain balances
  console.log("Balance breakdown by chain:");
  for (const [chain, balance] of Object.entries(data.chains)) {
    if (BigInt(balance) > 0n) {
      console.log(`  ${chain}: ${balance}`);
    }
  }

  // Example: Calculate percentage distribution
  const total = BigInt(data.total);
  if (total > 0n) {
    console.log("Distribution percentages:");
    for (const [chain, balance] of Object.entries(data.chains)) {
      const chainBalance = BigInt(balance);
      if (chainBalance > 0n) {
        const percentage = Number((chainBalance * 10000n) / total) / 100;
        console.log(`  ${chain}: ${percentage.toFixed(2)}%`);
      }
    }
  }
});

// Handle connection status
eventSource.addEventListener("accountId", (event) => {
  console.log(`Monitoring account: ${event.data}`);
});

// Handle heartbeats to detect connection issues
eventSource.addEventListener("heartbeat", () => {
  console.log("Connection alive");
});

// Error handling
eventSource.onerror = (error) => {
  console.error("EventSource error:", error);
  // Implement reconnection logic if needed
};

// Clean up when done
function closeConnection() {
  eventSource.close();
}
```

## Future Development Plans

We plan to extend the API in several directions:

1. **Additional Parachains**: Support for more parachains in the Polkadot ecosystem.
2. **Token Standards**: Support for various token standards beyond the Polkadot native token (DOT).
3. **Transaction Notifications**: Real-time notifications for incoming and outgoing transactions.
4. **Custom Filters**: Allowing clients to specify which chains and tokens to monitor.
5. **Identity Integration**: Integration with on-chain identity system from the Polkadot People Chain.

## Technical Requirements

- Cloudflare Workers account
- Node.js and Bun for development
- Wrangler CLI for deployment

## Development and Deployment

```bash
# Install dependencies
bun install

# Run locally
bun run dev

# Deploy to Cloudflare
bun run deploy
```
