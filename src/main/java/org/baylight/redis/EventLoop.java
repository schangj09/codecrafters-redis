package org.baylight.redis;

import java.io.IOException;
import java.net.Socket;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommandConstructor;
import org.baylight.redis.protocol.RespValueParser;

public class EventLoop {
    // keep a list of socket connections and continue checking for new connections
    private final RedisServiceBase service;
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    private final ExecutorService executor;
    private final RedisCommandConstructor commandConstructor;
    private final RespValueParser valueParser;
    private volatile boolean done = false;

    public EventLoop(RedisServiceBase service, RedisCommandConstructor commandConstructor,
            RespValueParser valueParser) {
        this.service = service;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for accepting new
                                                    // connections
        this.commandConstructor = commandConstructor;
        this.valueParser = valueParser;

        // create the thread for accepting new connections
        executor.execute(() -> {
            while (!done) {
                Socket clientSocket = null;
                try {
                    clientSocket = service.getServerSocket().accept();
                    ClientConnection conn = new ClientConnection(clientSocket);
                    clientSockets.add(conn);
                    System.out.println(
                            String.format("Connection accepted from client: %s, opened: %s",
                                    clientSocket, !clientSocket.isClosed()));
                } catch (IOException e) {
                    System.out.println("IOException on accept: " + e.getMessage());
                }
            }

            // loop was terminated so close any open connections
            for (ClientConnection conn : clientSockets) {
                try {
                    System.out.println(String.format("Closing connection to client: %s, opened: %s",
                            conn.clientSocket, !conn.clientSocket.isClosed()));
                    if (!conn.clientSocket.isClosed()) {
                        conn.clientSocket.close();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        });
    }

    public void terminate() {
        System.out.println(
                String.format("Terminate invoked. Closing %d connections.", clientSockets.size()));
        done = true;
        // stop accepting new connections and shut down the accept connections thread
        try {
            service.closeSocket();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        // executor close - waits for thread to finish closing all open connections
        executor.close();
    }

    public void processLoop() throws InterruptedException {
        while (!done) {
            // check for bytes on next socket and process
            boolean didProcess = false;
            Iterator<ClientConnection> iter = clientSockets.iterator();
            System.out.println(String.format("Check pending replication: %s (%s)",
                    service.isReplicationFromLeaderPending(), service.getClass().getSimpleName()));
            if (!service.isReplicationFromLeaderPending()) {
                for (; iter.hasNext();) {
                    ClientConnection conn = iter.next();
                    if (conn.isClosed()) {
                        System.out.println(String.format("Connection closed by client: %s",
                                conn.clientSocket));
                        iter.remove();
                        continue;
                    }

                    try {
                        while (conn.reader.available() > 0) {
                            RedisCommand command = commandConstructor
                                    .newCommandFromValue(valueParser.parse(conn.reader));
                            didProcess = true;
                            if (command != null) {
                                process(conn, command);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println(String.format("EventLoop Exception: %s \"%s\"",
                                e.getClass().getSimpleName(), e.getMessage()));
                    }
                }
            }
            // sleep a bit if there were no lines processed
            if (!didProcess) {
                // System.out.println("sleep 1s");
                Thread.sleep(80L);
            }
        }
    }

    void process(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received client command: %s", command));

        service.execute(command, conn);
        switch (command) {
        case EofCommand c -> {
            conn.clientSocket.close();
        }
        case TerminateCommand c -> {
            terminate();
        }
        default -> {
            // no action for other command types
        }
        }
    }
}
