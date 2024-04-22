package org.baylight.redis;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommandConstructor;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;
import org.baylight.redis.streams.IllegalStreamItemIdException;
import org.baylight.redis.streams.RedisStreamData;
import org.baylight.redis.streams.StreamId;
import org.baylight.redis.streams.StreamValue;
import org.baylight.redis.streams.StreamsWaitManager;

public abstract class RedisServiceBase implements ReplicationServiceInfoProvider {

    private static final Set<String> DEFAULT_SECTIONS = Set.of("server", "replication", "stats",
            "replication-graph");

    private ServerSocket serverSocket;
    private EventLoop eventLoop;
    private final RedisCommandConstructor commandConstructor;
    private final RespValueParser valueParser;
    private final ExecutorService connectionsExecutorService;
    private final ExecutorService commandsExecutorService;
    private final ConnectionManager connectionManager;
    private volatile boolean done = false;
    private final RedisServiceOptions options;
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
        this.options = options;
        this.port = options.getPort();
        this.role = options.getRole();
        this.clock = clock;
        commandConstructor = new RedisCommandConstructor();
        valueParser = new RespValueParser();

        // Thread pool of size 2 - one for accepting new client connections, one for reading values
        // from those clients in the ConnectionManager
        connectionsExecutorService = Executors.newFixedThreadPool(2);
        // Use a cached thread pool for executing blocking commands
        commandsExecutorService = Executors.newCachedThreadPool();

        // read database
        if (options.getDbfilename() != null) {
            try {
                File dbFile = new File(options.getDir(), options.getDbfilename());
                // only read the file if it exists
                if (dbFile.exists()) {
                    DatabaseReader reader = new DatabaseReader(dbFile, dataStoreMap, clock);
                    reader.readDatabase();
                } else {
                    System.out.println(String.format("Database file %s does not exist",
                            dbFile.getAbsolutePath()));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        connectionManager = new ConnectionManager();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.out.println("Server started. Listening on Port " + port);

        eventLoop = new EventLoop(this, commandConstructor);

        // create the thread for accepting new connections
        connectionsExecutorService.execute(() -> {
            while (!done) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    clientSocket.setKeepAlive(true);
                    clientSocket.setSoTimeout(0); // infinite timeout

                    ClientConnection conn = new ClientConnection(clientSocket, valueParser);
                    connectionManager.addConnection(conn);
                    System.out.println(
                            String.format("Connection accepted from client: %s, opened: %s", conn,
                                    !conn.isClosed()));
                } catch (IOException e) {
                    System.out.println("IOException on accept: " + e.getMessage());
                }
            }

            // loop was terminated so close any open connections
            connectionManager.closeAllConnections();
        });

        // start thread to read from client connections
        connectionManager.start(connectionsExecutorService);
    }

    public void closeSocket() throws IOException {
        serverSocket.close();
    }

    public int getPort() {
        return port;
    }

    public String getConfig(String configName) {
        // Note: returns null for unknown config name
        return options.getConfigValue(configName);
    }

    public void shutdown() {
        connectionsExecutorService.shutdown();
        commandsExecutorService.shutdown();
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

    public RespSimpleStringValue getType(String key) {
        if (dataStoreMap.containsKey(key)) {
            return dataStoreMap.get(key).getType().getTypeResponse();
        } else {
            return new RespSimpleStringValue("none");
        }
    }

    public StoredData set(String key, StoredData storedData) {
        return dataStoreMap.put(key, storedData);
    }

    public StreamId xadd(String key, String itemId, RespValue[] itemMap)
            throws IllegalStreamItemIdException {
        StoredData storedData = dataStoreMap.computeIfAbsent(key,
                (k) -> new StoredData(new RedisStreamData(k), clock.millis(), null));
        return storedData.getStreamValue().add(itemId, clock, itemMap);
    }

    public List<StreamValue> xrange(String key, String start, String end)
            throws IllegalStreamItemIdException {
        StoredData storedData = dataStoreMap.computeIfAbsent(key,
                (k) -> new StoredData(new RedisStreamData(k), clock.millis(), null));
        return storedData.getStreamValue().queryRange(start, end);
    }

    public List<List<StreamValue>> xread(
            List<String> keys, List<String> startValues, Long timeoutMillis)
            throws IllegalStreamItemIdException {
        Map<String, RedisStreamData> streams = keys.stream()
                .collect(Collectors.toMap(
                        s -> s,
                        s -> dataStoreMap.computeIfAbsent(s,
                                (k) -> new StoredData(new RedisStreamData(k), clock.millis(), null))
                                .getStreamValue()));
        Map<String, StreamId> startIds = new HashMap<>();
        int i = 0;
        for (String s : keys) {
            startIds.put(s, streams.get(s).getStreamIdForRead(startValues.get(i++)));
        }
        Map<String, List<StreamValue>> values = StreamsWaitManager.INSTANCE.readWithWait(streams,
                startIds, 0, clock, timeoutMillis == null ? 1L : timeoutMillis);
        return keys.stream().map(values::get).toList();
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

    public abstract void execute(RedisCommand command, ClientConnection conn) throws IOException;

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

    public byte[] replicationConfirm(ClientConnection connection, Map<String, RespValue> optionsMap,
            long startBytesOffset) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public abstract byte[] replicationConfirm(Map<String, RespValue> optionsMap,
            long startBytesOffset);

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

    public void runCommandLoop() throws InterruptedException {
        eventLoop.runCommandLoop();
    }

    public void terminate() {
        System.out.println(String.format("Terminate invoked. Closing %d connections.",
                connectionManager.getNumConnections()));
        eventLoop.terminate();
        done = true;
        // stop accepting new connections and shut down the accept connections thread
        try {
            closeSocket();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        shutdown();
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    void executeCommand(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received client command: %s", command));

        if (command.isBlockingCommand()) {
            commandsExecutorService.submit(() -> {
                try {
                    execute(command, conn);
                } catch (Exception e) {
                    System.out.println(String.format(
                            "EventLoop Exception: %s \"%s\"",
                            e.getClass().getSimpleName(), e.getMessage()));
                    e.printStackTrace();
                }
            });
        } else {
            execute(command, conn);
        }

        switch (command) {
        case EofCommand c -> {
            conn.close();
        }
        case TerminateCommand c -> {
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

        public void runCommandLoop() throws InterruptedException {
            while (!done) {
                // check for a value on one of the client sockets and process it as a command
                boolean didProcess = service.getConnectionManager().getNextValue((conn, value) -> {
                    RedisCommand command = commandConstructor.newCommandFromValue(value);
                    if (command != null) {
                        try {
                            service.executeCommand(conn, command);
                        } catch (Exception e) {
                            System.out.println(String.format(
                                    "EventLoop Exception: %s \"%s\"",
                                    e.getClass().getSimpleName(), e.getMessage()));
                            e.printStackTrace();
                            // since this is a blocking command, we better return an error response
                            conn.sendError(e.getMessage());
                        }
                    }
                });

                // sleep a bit if there were no commands to be processed
                if (!didProcess) {
                    // System.out.println("sleep 1s");
                    Thread.sleep(80L);
                }
            }
        }
    }

    public Collection<String> getKeys() {
        return dataStoreMap.keySet();
    }

}
