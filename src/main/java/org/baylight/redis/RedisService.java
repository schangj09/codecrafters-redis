package org.baylight.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.baylight.redis.commands.RedisCommand;

public class RedisService {
    private static final int PORT = 6379;
    private ServerSocket serverSocket;
    private final Map<String, byte[]> dataStoreMap = new ConcurrentHashMap<>();

    public RedisService() { 
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);
        System.out.println("Server started. Listening on Port " + PORT);
    }

    public void closeSocket() throws IOException {
        serverSocket.close();
    }

    public void shutdown() {
    }

    public byte[] get(String key) { 
        return dataStoreMap.get(key);
    }
    public byte[] set(String key, byte[] value) {
        return dataStoreMap.put(key, value);
    }
    public void delete(String key) {
        dataStoreMap.remove(key);
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public Map<String, byte[]> getDataStoreMap() {
        return dataStoreMap;
    }

    public byte[] execute(RedisCommand command) {
        return command.execute(this);
    }

}
