import dotenv from "dotenv";
dotenv.config();
import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import { EventHubConsumerClient } from "@azure/event-hubs";

const connectionString = process.env.CONNECTION_STRING
const eventHubName = process.env.NAME;
const consumerGroup = "$Default";

// Track connected WebSocket clients
const clients = new Set();

async function main() {
  const client = new EventHubConsumerClient(
    consumerGroup,
    connectionString,
    eventHubName
  );

  console.log("📡 Listening for Event Hub messages...");

  const subscription = client.subscribe({
    processEvents: async (events, context) => {
      const start = Date.now();

      for (const event of events) {
        const rawString = event.body.toString();

        const result = {
          ENTITLEMENT_CODE_6: "",
          DELAY_MINS_269: "",
          BID_PRICE_12: "",
          ASK_PRICE_10: "",
          CONTRIBUTOR_ID_248: "",
          CITY_ID_810: "",
          CITY_CODE_1271: "",
          REGION_ID_811: "",
          REGION_CODE_1270: "",
          TRADE_PRICE_8: "",
          TRADE_TREND_432: "",
          TRADE_TICK_316: "",
          CHG_361: "",
          QUOTE_OFFICIAL_DATE_824: "",
          QUOTE_OFFICIAL_TIME_25: "",
          DESKTOP_ELIGIBILITY_IND_1662: "",
          ACTIVITY_DATETIME_16: " ",
          QUOTE_DATETIME_20: "",
          TRADE_DATETIME_18: "",
          PCT_CHG_362: "",
          CONFLATE_INDICATOR_5201: "",
          ENUM_SRC_ID_4: "",
          SYMBOL_TICKER_5: "",
          FRACTIONAL_IND_670: "",
          CURRENCY_STRING_435: "",
          TRADE_OPEN_400: "",
          TRADE_HIGH_388: "",
          TRADE_LOW_394: "",
          CURRENT_PRICE_14: "",
          YEST_TRADE_CLOSE_407: "",
          DISPLAY_PRECISION_280: "",
          PREV_TRADE_DATE_451: "",
          RECORD_STALE_IND_5076: ""
        };

        const lines = rawString.split("\n");

        for (let line of lines) {
          const [key, value] = line.split("=");
          if (key && value !== undefined) {
            const transformedKey = key
              .replace(/\./g, "_")
              .replace(/\((\d+)\)/, "_$1");
            result[transformedKey] = value;
          }
        }

        const sequenceNumber = event.sequenceNumber;
        const offset = event.offset;
        const enqueuedTime = event.enqueuedTimeUtc;
        const partitionId = context.partitionId;
        const end = Date.now();

        result["META_INFO"] = {
          sequenceNumber,
          offset,
          enqueuedTime,
          partitionId,
          start,
          end,
          difference: `${end - start} ms`
        };

        console.log(result);

        // 🔁 Broadcast to all WebSocket clients
        for (const client of clients) {
          if (client.readyState === client.OPEN) {
            client.send(JSON.stringify(result));
          }
        }
      }
    },
    processError: async (err, context) => {
      console.error("❌ Error processing events:", err);
    }
  });
}

// 🛠 Express + HTTP + WS Setup
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

// Upgrade HTTP request to WS only on `/stream`
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/stream") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

// Store and clean up connected clients
wss.on("connection", (ws) => {
  console.log("✅ WebSocket connected");
  clients.add(ws);

  ws.on("close", () => {
    clients.delete(ws);
    console.log("❌ WebSocket disconnected");
  });
});

main().catch((err) => {
  console.error("❌ Error in main():", err);
});

server.listen(8080, () => {
  console.log("🚀 Server running at http://localhost:8080");
  console.log("📡 WS endpoint at ws://localhost:8080/stream");
});
