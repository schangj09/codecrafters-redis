package org.baylight.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.baylight.redis.commands.RedisCommand;

public class RedisService {
    
    private ServerSocket serverSocket;
    private final int port;
    private final Clock clock;
    private final Map<String, StoredData> dataStoreMap = new ConcurrentHashMap<>();

    public RedisService(int port, Clock clock) {
        this.port = port;
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

}
