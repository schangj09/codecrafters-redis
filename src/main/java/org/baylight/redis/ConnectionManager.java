package org.baylight.redis;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.BiConsumer;

import org.baylight.redis.protocol.RespValue;

public class ConnectionManager {
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();

    public ConnectionManager() {

    }

    public void addConnection(ClientConnection conn) {
        clientSockets.add(conn);
    }

    public void closeAllConnections() {
        for (ClientConnection conn : clientSockets) {
            try {
                System.out.println(String.format("Closing connection to client: %s, opened: %s",
                        conn, !conn.isClosed()));
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    public int getNumConnections() {
        return clientSockets.size();
    }

    public void getNextValue(BiConsumer<ClientConnection, RespValue> valueHandler) {
        Iterator<ClientConnection> iter = clientSockets.iterator();
        for (; iter.hasNext();) {
            ClientConnection conn = iter.next();
            if (conn.isClosed()) {
                System.out.println(String.format("Connection closed by the server: %s", conn));
                iter.remove();
                continue;
            } else if (conn.isFollowerHandshakeComplete()) {
                System.out.println(String.format(
                        "ConnectionManager: no longer listening to commands from client after follower connection handshake complete: %s",
                        conn));
                iter.remove();
                continue;
            }

            try {
                while (conn.available() > 0) {
                    System.out.println(String.format(
                            "ConnectionManager: about to read from connection, available: %d %s",
                            conn.available(), conn));

                    RespValue value = conn.readValue();
                    valueHandler.accept(conn, value);
                }
            } catch (Exception e) {
                System.out.println(String.format("ConnectionManager Exception: %s \"%s\"",
                        e.getClass().getSimpleName(), e.getMessage()));
            }
        }
    }
}
