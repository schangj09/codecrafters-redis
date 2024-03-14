package org.baylight.redis;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.baylight.redis.protocol.RespValue;

public class ConnectionManager {
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    private final Map<ClientConnection, Queue<RespValue>> clientValues = new ConcurrentHashMap<>();

    public ConnectionManager() {

    }

    public void start(ExecutorService executorService) throws IOException {
        // TODO implement orderly shutdown for this thread
        executorService.submit(() -> {
            for (;;) {
                boolean didRead = false;
                Iterator<ClientConnection> iter = clientSockets.iterator();
                for (; iter.hasNext();) {
                    ClientConnection conn = iter.next();
                    if (conn.isClosed()) {
                        System.out.println(
                                String.format("Connection closed by the server: %s", conn));
                        clientValues.remove(conn);
                        iter.remove();
                    }
                    while (conn.available() > 0) {
                        didRead = true;
                        System.out.println(String.format(
                                "ConnectionManager: about to read from connection, available: %d %s",
                                conn.available(), conn));
                        RespValue value = null;
                        try {
                            value = conn.readValue();
                        } catch (Exception e) {
                            System.out.println(String.format(
                                    "ConnectionManager read exception conn: %s %s \"%s\"", conn,
                                    e.getClass().getSimpleName(), e.getMessage()));
                        }
                        if (value != null) {
                            getClientValuesQueue(conn).offer(value);
                        }
                    }
                }
                // if there was nothing to be read, then sleep a little
                if (!didRead) {
                    // System.out.println("sleep 1s");
                    Thread.sleep(80L);
                }
            }
        });
    }

    private Queue<RespValue> getClientValuesQueue(ClientConnection conn) {
        return clientValues.computeIfAbsent(conn, (key) -> new ConcurrentLinkedQueue<RespValue>());
    }

    public void addConnection(ClientConnection conn) {
        clientSockets.addLast(conn);
    }

    public void addPriorityConnection(ClientConnection priorityConnection) {
        // for followers that listen to a leader, the leader connection should be the first
        // connection in the queue so getNextValue will prioritize replication commands
        // from the leader before commands from other clients
        clientSockets.addFirst(priorityConnection);
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

    public boolean getNextValue(BiConsumer<ClientConnection, RespValue> valueHandler) {
        Iterator<ClientConnection> iter = clientSockets.iterator();
        boolean foundValue = false;
        for (; !foundValue && iter.hasNext();) {
            ClientConnection conn = iter.next();
            Queue<RespValue> valuesQueue = getClientValuesQueue(conn);
            try {
                if (!valuesQueue.isEmpty()) {
                    RespValue value = valuesQueue.poll();
                    valueHandler.accept(conn, value);
                    foundValue = true;
                }
            } catch (Exception e) {
                System.out.println(
                        String.format("ConnectionManager nextValue exception conn: %s %s \"%s\"",
                                conn, e.getClass().getSimpleName(), e.getMessage()));
            }
        }
        return foundValue;
    }

}
