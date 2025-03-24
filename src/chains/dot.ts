import { DurableObject } from "cloudflare:workers";
import { generate } from "@babia/uuid-v7";
import {
  dot,
  hydration,
  polimec,
  XcmV3Junctions,
} from "@polkadot-api/descriptors";
import {
  type PolkadotClient,
  type SS58String,
  type TypedApi,
  createClient,
} from "polkadot-api";
import { withPolkadotSdkCompat } from "polkadot-api/polkadot-sdk-compat";
import { getWsProvider } from "polkadot-api/ws-provider/web";
import { SSEMessage } from "../SSEMessage";
import { Balance } from "../Balance";

export class Listener extends DurableObject<Env> {
  private polkadotClient: PolkadotClient;
  private polkadotApi: TypedApi<typeof dot>;

  private polimecClient: PolkadotClient;
  private polimecApi: TypedApi<typeof polimec>;

  private hydrationClient: PolkadotClient;
  private hydrationApi: TypedApi<typeof hydration>;

  private balance: Balance;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.polkadotClient = createClient(
      withPolkadotSdkCompat(
        getWsProvider([
          "wss://rpc.ibp.network/polkadot",
          "wss://polkadot.dotters.network",
        ])
      )
    );
    this.polkadotApi = this.polkadotClient.getTypedApi(dot);

    this.polimecClient = createClient(
      withPolkadotSdkCompat(getWsProvider(["wss://rpc.polimec.org"]))
    );
    this.polimecApi = this.polimecClient.getTypedApi(polimec);

    this.hydrationClient = createClient(
      withPolkadotSdkCompat(getWsProvider(["wss://hydration.ibp.network"]))
    );
    this.hydrationApi = this.hydrationClient.getTypedApi(hydration);

    this.balance = new Balance();
  }

  async subscribe(accountId: SS58String) {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();

    const message = new SSEMessage(accountId, "accountId", generate());
    writer.write(message.encode());

    const dotSubscription = this.polkadotApi.query.System.Account.watchValue(
      accountId
    ).subscribe({
      next: (content) => {
        try {
          if (content.data.free > 0n) {
            this.balance.setPolkadot(content.data.free);
            const total = this.balance.total();
            const message = new SSEMessage(total, "DOT", generate());
            writer.write(message.encode());
          }
        } catch (err) {
          console.error(
            "Failed to write Polkadot balance update to stream:",
            err
          );
        }
      },
      error: (err) => {
        console.error("Polkadot observable error:", err);
        dotSubscription.unsubscribe();
      },
      complete: () => {
        console.log("Polkadot observable completed");
        dotSubscription.unsubscribe();
      },
    });

    const polimecSubscription =
      this.polimecApi.query.ForeignAssets.Account.watchValue(
        { parents: 1, interior: XcmV3Junctions.Here() },
        accountId
      ).subscribe({
        next: (content) => {
          try {
            if (!content) {
              console.error("Polimec content is undefined");
              return;
            }
            if (content.balance > 0n) {
              this.balance.setPolimec(content.balance);
              const total = this.balance.total();
              const message = new SSEMessage(total, "DOT", generate());
              writer.write(message.encode());
            }
          } catch (err) {
            console.error(
              "Failed to write Polimec balance update to stream:",
              err
            );
          }
        },
        error: (err) => {
          console.error("Polimec observable error:", err);
          polimecSubscription.unsubscribe();
        },
        complete: () => {
          console.log("Polimec observable completed");
          polimecSubscription.unsubscribe();
        },
      });

    const hydrationSubscription =
      this.hydrationApi.query.Tokens.Accounts.watchValue(
        accountId,
        5
      ).subscribe({
        next: (content) => {
          try {
            if (!content) {
              console.error("Hydration content is undefined");
              return;
            }
            if (content.free > 0n) {
              this.balance.setHydration(content.free);
              const total = this.balance.total();
              const message = new SSEMessage(total, "DOT", generate());
              writer.write(message.encode());
            }
          } catch (err) {
            console.error(
              "Failed to write Hydration balance update to stream:",
              err
            );
          }
        },
        error: (err) => {
          console.error("Hydration observable error:", err);
          hydrationSubscription.unsubscribe();
        },
        complete: () => {
          console.log("Hydration observable completed");
          hydrationSubscription.unsubscribe();
        },
      });

    const heartbeat = setInterval(async () => {
      try {
        const message = new SSEMessage(
          new Date().toString(),
          "heartbeat",
          generate()
        );
        writer.write(message.encode());
      } catch (err) {
        console.error("Failed to send heartbeat:", err);
        clearInterval(heartbeat);
      }
    }, 30_000);

    return readable;
  }
}
