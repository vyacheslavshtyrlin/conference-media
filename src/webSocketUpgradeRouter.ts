import type { Server } from "node:http";
import type { Duplex } from "node:stream";
import type { WebSocketServer } from "ws";

export function attachWebSocketUpgradeRouter(
  server: Server,
  routes: ReadonlyMap<string, WebSocketServer>,
): void {
  server.on("upgrade", (request, socket, head) => {
    const pathname = parsePathname(request.url);
    const webSocketServer = pathname ? routes.get(pathname) : undefined;
    if (!webSocketServer) {
      rejectUpgrade(socket);
      return;
    }
    webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
      webSocketServer.emit("connection", webSocket, request);
    });
  });
}

function parsePathname(rawUrl: string | undefined): string | undefined {
  try {
    return new URL(rawUrl ?? "", "http://localhost").pathname;
  } catch {
    return undefined;
  }
}

function rejectUpgrade(socket: Duplex): void {
  if (socket.writable) {
    socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
  }
  socket.destroy();
}
