import { getSs58AddressInfo } from "polkadot-api";
import { Listener } from "./chains/dot";
import { Hono } from "hono";
import { cors } from "hono/cors";

const app = new Hono<{ Bindings: Env }>();

app.use("/*", cors());

app.get("/:accountId", async (c) => {
  const accountId = c.req.param("accountId");
  if (!accountId) {
    return c.text("Account ID is required", 400);
  }

  const { isValid } = getSs58AddressInfo(accountId);
  if (!isValid) {
    return c.text("Invalid Account Provided", 400);
  }
  // TODO: We probably want two have 2 different DO.
  // 1. One is listening for on-chain storage changes. So we can have 1 DO per chain.
  // 2. The other is streaming and maintaining these changes to the client(s). So we can have 1 DO per client connected to thew DO #1.
  // Every unique ID refers to an individual instance of the Durable Object class
  const id = c.env.LISTENER.idFromName("polkadot");
  const stub = c.env.LISTENER.get(id);
  // Methods on the Durable Object are invoked via the stub
  const rpcResponse = await stub.subscribe(accountId);

  // @ts-expect-error The RPC Method returns a ReadableStream, and the Response constructor expects a ReadableStream. I have to investigate this.
  return new Response(rpcResponse, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
});

export default app;

export { Listener };
