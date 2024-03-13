package org.baylight.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommandConstructor;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public abstract class RedisServiceBase implements ReplicationServiceInfoProvider {

    private static final Set<String> DEFAULT_SECTIONS = Set.of("server", "replication", "stats",
            "replication-graph");

    private ServerSocket serverSocket;
    private EventLoop eventLoop;
    private final RedisCommandConstructor commandConstructor;
    private final RespValueParser valueParser;
    private final ExecutorService connectionAcceptExecutorService;
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    private volatile boolean done = false;
    private final int port;
    private final String role;
    private final Clock clock;
    private final Map<String, StoredData> dataStoreMap = new ConcurrentHashMap<>();

    public static RedisServiceBase newInstance(RedisServiceOptions options, Clock clock) {
        String role = options.getRole();
        return switch (role) {
        case RedisConstants.FOLLOWER -> new FollowerService(options, clock);
        case RedisConstants.LEADER -> new LeaderService(options, clock);
        default -> throw new UnsupportedOperationException(
                "Unexpected role type for new Redis service: " + role);
        };
    }

    protected RedisServiceBase(RedisServiceOptions options, Clock clock) {
        this.port = options.getPort();
        this.role = options.getRole();
        this.clock = clock;
        commandConstructor = new RedisCommandConstructor();
        valueParser = new RespValueParser();

        // Thread pool of size 1 for accepting new connections
        connectionAcceptExecutorService = Executors.newFixedThreadPool(1);
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.out.println("Server started. Listening on Port " + port);

        eventLoop = new EventLoop(this, commandConstructor);

        // create the thread for accepting new connections
        connectionAcceptExecutorService.execute(() -> {
            while (!done) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    clientSocket.setKeepAlive(true);
                    clientSocket.setSoTimeout(0); // infinite timeout

                    ClientConnection conn = new ClientConnection(clientSocket, valueParser);
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
                            conn, !conn.isClosed()));
                    if (!conn.isClosed()) {
                        conn.close();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        });
    }

    public void closeSocket() throws IOException {
        serverSocket.close();
    }

    public int getPort() {
        return port;
    }

    public void shutdown() {
    }

    public boolean containsKey(String key) {
        return dataStoreMap.containsKey(key);
    }

    public boolean containsUnexpiredKey(String key) {
        StoredData storedData = dataStoreMap.getOrDefault(key, null);
        return storedData != null && !isExpired(storedData);
    }

    public StoredData get(String key) {
        return dataStoreMap.get(key);
    }

    public StoredData set(String key, StoredData storedData) {
        return dataStoreMap.put(key, storedData);
    }

    public void delete(String key) {
        dataStoreMap.remove(key);
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public Map<String, StoredData> getDataStoreMap() {
        return dataStoreMap;
    }

    public abstract void execute(RedisCommand command, ClientConnection conn, boolean writeResponse)
            throws IOException;

    public boolean isExpired(StoredData storedData) {
        long now = clock.millis();
        return storedData.isExpired(now);
    }

    public long getCurrentTime() {
        return clock.millis();
    }

    public String info(Map<String, RespValue> optionsMap) {

        StringBuilder sb = new StringBuilder();
        if (infoSection(optionsMap, "server")) {
            sb.append("# Redis server info\n");
            sb.append("redis_version:").append("3.2.0-org-baylight").append("\n");
        }

        // replication section
        if (infoSection(optionsMap, "replication")) {
            sb.append("# Replication\n");
            sb.append("role:").append(role).append("\n");
            getReplcationInfo(sb);
        }
        return sb.toString();
    }

    private boolean infoSection(Map<String, RespValue> optionsMap, String section) {
        return optionsMap.containsKey("all") || optionsMap.containsKey("everything")
                || (optionsMap.size() == 1 && isDefault(section))
                || (optionsMap.containsKey("default") && isDefault(section))
                // || (optionsMap.containsKey("server") && isServer(section))
                // || (optionsMap.containsKey("clients") && isClients(section))
                // || (optionsMap.containsKey("memory") && isMemory(section))
                || optionsMap.containsKey(section);
    }

    private boolean isDefault(String section) {
        return DEFAULT_SECTIONS.contains(section);
    }

    public abstract byte[] replicationConfirm(Map<String, RespValue> optionsMap);

    /**
     * Wait until replication has caught up for the given number of replicas. The service must block
     * on this method until either the number of requested replicas has been reached or the timeout
     * has been reached.
     * 
     * @param numReplicas   the requested number of replicas
     * @param timeoutMillis the timeout in milliseconds. If the timeout is reached, the method
     *                      returns the number of replicas that have caught up. If the timeout is
     *                      zero, the method returns immediately. If the timeout is negative, an
     *                      {@link IllegalArgumentException}
     * @return the number of replicas that have caught up. This may be less than the number of
     *         replicas requested.
     */
    public abstract int waitForReplicationServers(int numReplicas, long timeoutMillis);

    public byte[] psync(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        return RespConstants.OK;
    }

    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        throw new UnsupportedOperationException("no psync rdb implementation for the service");
    }

    /**
     * For a follower service, this method returns true if replication is pending. For a leader
     * service, this method returns false.
     * 
     * @return
     */
    public boolean isReplicationFromLeaderPending() {
        return false;
    }

    public void processMainLoop() throws InterruptedException {
        eventLoop.processLoop();
    }

    public void terminate() {
        System.out.println(
                String.format("Terminate invoked. Closing %d connections.", clientSockets.size()));
        done = true;
        // stop accepting new connections and shut down the accept connections thread
        try {
            closeSocket();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        // executor close - waits for thread to finish closing all open connections
        this.connectionAcceptExecutorService.close();
    }

    public Collection<ClientConnection> getClientSockets() {
        return clientSockets;
    }

    void executeCommand(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received client command: %s", command));

        execute(command, conn, true);
        switch (command) {
        case EofCommand c -> {
            conn.close();
        }
        case TerminateCommand c -> {
            eventLoop.terminate();
            terminate();
        }
        default -> {
            // no action for other command types
        }
        }
    }

    static class EventLoop {
        // keep a list of socket connections and continue checking for new connections
        private final RedisServiceBase service;
        private final RedisCommandConstructor commandConstructor;
        private volatile boolean done = false;

        public EventLoop(RedisServiceBase service, RedisCommandConstructor commandConstructor) {
            this.service = service;
            this.commandConstructor = commandConstructor;
        }

        public void terminate() {
            done = true;
        }

        public void processLoop() throws InterruptedException {
            while (!done) {
                // check for bytes on next socket and process
                boolean didProcess = false;
                Iterator<ClientConnection> iter = service.getClientSockets().iterator();
                if (!service.isReplicationFromLeaderPending()) {
                    for (; iter.hasNext();) {
                        ClientConnection conn = iter.next();
                        if (conn.isClosed()) {
                            System.out.println(
                                    String.format("Connection closed by the server: %s", conn));
                            iter.remove();
                            continue;
                        } else if (conn.isFollowerHandshakeComplete()) {
                            System.out.println(String.format(
                                    "EventLoop: no longer listening to commands from client after follower connection handshake complete: %s",
                                    conn));
                            iter.remove();
                            continue;
                        }

                        try {
                            while (conn.available() > 0) {
                                System.out.println(String.format(
                                        "EventLoop: about to read from connection, available: %d %s",
                                        conn.available(), conn));

                                RespValue value = conn.readValue();
                                RedisCommand command = commandConstructor
                                        .newCommandFromValue(value);
                                didProcess = true;
                                if (command != null) {
                                    service.executeCommand(conn, command);
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
    }
}
