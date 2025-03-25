import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { getSs58AddressInfo } from 'polkadot-api';
import { Listener } from './durable';

// Create Hono app
const app = new Hono<{ Bindings: Env }>();

// Apply CORS middleware to all routes
app.use('/*', cors());

/**
 * Endpoint to subscribe to balance updates for an account
 */
app.get('/:accountId', async (c) => {
  const accountId = c.req.param('accountId');

  // Validate account ID
  if (!accountId) {
    return c.text('Account ID is required', 400);
  }

  // Validate SS58 address format
  const { isValid } = getSs58AddressInfo(accountId);
  if (!isValid) {
    return c.text('Invalid Account Provided', 400);
  }

  try {
    // TODO: We probably want two have 2 different DO classes:
    // ChainListenerDO: One DO per chain that subscribes to chain events and maintains state.
    // ClientConnectionDO: One DO per client that receives updates from ChainListenerDOs.

    // Create a Durable Object stub for this client
    const id = c.env.LISTENER.idFromName(`account:${accountId}`);
    const stub = c.env.LISTENER.get(id);

    // Call the subscribe RPC method
    const stream = await stub.subscribe(accountId);

    // Return the stream as an SSE response
    // @ts-expect-error: The DO returns a ReadableStream, and the Response constructor accepts it. TODO: Investigate why TS is complaining.
    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      },
    });
  } catch (error) {
    console.error('Error setting up subscription:', error);
    return c.text('Internal Server Error', 500);
  }
});

// Export the app as the default export
export default app;

// Export the Listener Durable Object
export { Listener };
