import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket, WebSocketServer } from "ws";

import { attachWebSocketUpgradeRouter } from "./webSocketUpgradeRouter.js";

const cleanup: Array<() => Promise<void>> = [];

afterEach(async () => {
  await Promise.all(cleanup.splice(0).map((close) => close()));
});

describe("attachWebSocketUpgradeRouter", () => {
  it("routes public and private paths to distinct noServer instances and rejects unknown upgrades", async () => {
    const server = createServer();
    const publicServer = new WebSocketServer({ noServer: true });
    const privateServer = new WebSocketServer({ noServer: true });
    const publicRequests: string[] = [];
    const privateRequests: string[] = [];
    publicServer.on("connection", (_socket, request) => publicRequests.push(request.url ?? ""));
    privateServer.on("connection", (_socket, request) => privateRequests.push(request.url ?? ""));
    attachWebSocketUpgradeRouter(server, new Map([
      ["/ws", publicServer],
      ["/internal/recording-bot/v1", privateServer],
    ]));
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    const sockets: WebSocket[] = [];
    cleanup.push(async () => {
      for (const socket of sockets) socket.terminate();
      await Promise.all([closeWebSocketServer(publicServer), closeWebSocketServer(privateServer)]);
      await new Promise<void>((resolve) => server.close(() => resolve()));
    });

    sockets.push(await connect(`ws://127.0.0.1:${port}/ws?token=browser-token`));
    sockets.push(await connect(`ws://127.0.0.1:${port}/internal/recording-bot/v1?admission=private-token`));
    const unknownStatus = await rejectedUpgradeStatus(`ws://127.0.0.1:${port}/unknown?admission=must-not-route`);

    expect(publicRequests).toEqual(["/ws?token=browser-token"]);
    expect(privateRequests).toEqual(["/internal/recording-bot/v1?admission=private-token"]);
    expect(unknownStatus).toBe(404);
  });
});

function connect(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.once("open", () => resolve(socket));
    socket.once("error", reject);
  });
}

function rejectedUpgradeStatus(url: string): Promise<number | undefined> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.once("unexpected-response", (_request, response) => {
      response.resume();
      resolve(response.statusCode);
    });
    socket.once("error", reject);
  });
}

function closeWebSocketServer(server: WebSocketServer): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}
