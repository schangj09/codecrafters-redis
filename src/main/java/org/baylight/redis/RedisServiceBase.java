package org.baylight.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public abstract class RedisServiceBase implements ReplicationServiceInfoProvider {

    private static final Set<String> DEFAULT_SECTIONS = Set.of(
            "server",
            "replication",
            "stats",
            "replication-graph");
    private ServerSocket serverSocket;
    private final int port;
    private final String role;
    private final Clock clock;
    private final Map<String, StoredData> dataStoreMap = new ConcurrentHashMap<>();

    public static RedisServiceBase newInstance(RedisServiceOptions options, Clock clock) {
        String role = options.getRole();
        return switch (role) {
            case RedisConstants.FOLLOWER ->
                new FollowerService(options, clock);
            case RedisConstants.LEADER ->
                new LeaderService(options, clock);
            default ->
                throw new UnsupportedOperationException("Unexpected role type for new Redis service: " + role);
        };
    }

    protected RedisServiceBase(RedisServiceOptions options, Clock clock) {
        this.port = options.getPort();
        this.role = options.getRole();
        this.clock = clock;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        System.out.println("Server started. Listening on Port " + port);
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

    public byte[] execute(RedisCommand command) {
        return command.execute(this);
    }

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
        return optionsMap.containsKey("all")
                || optionsMap.containsKey("everything")
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

    public byte[] replicationConfirm(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        return RespConstants.OK;
    }

	public byte[] psync(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        return RespConstants.OK;
	}

    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        // TODO make this abstract once leader and follower both override this method
        throw new UnsupportedOperationException("no psync rdb implementation for the service");
	}
}
