package org.baylight.redis;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.Map;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public class FollowerService extends RedisServiceBase {
    private ConnectionToLeader leaderConnection;
    private final String leaderHost;
    private final int leaderPort;
    private Socket leaderClientSocket;

    public FollowerService(RedisServiceOptions options, Clock clock) {
        super(options, clock);

        leaderHost = options.getReplicaof();
        leaderPort = options.getReplicaofPort();
    }

    @Override
    public void getReplcationInfo(StringBuilder sb) {
        // nothing to add for now
    }

    @Override
    public void start() throws IOException {
        super.start();

        leaderClientSocket = new Socket(leaderHost, leaderPort);
        leaderClientSocket.setReuseAddress(true);
        leaderConnection = new ConnectionToLeader(this);

        // initiate the handshake with the leader service
        leaderConnection.startHandshake();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (leaderConnection != null) {
            leaderConnection.terminate();
        }
    }

    /**
     * @return the leaderConnection
     */
    public ConnectionToLeader getLeaderConnection() {
        return leaderConnection;
    }

    /**
     * @return the leaderHost
     */
    public String getLeaderHost() {
        return leaderHost;
    }

    /**
     * @return the leaderPort
     */
    public int getLeaderPort() {
        return leaderPort;
    }

    /**
     * @return the leaderClientSocket
     */
    public Socket getLeaderClientSocket() {
        return leaderClientSocket;
    }

    @Override
    public boolean isReplicationFromLeaderPending() {
        return leaderConnection.isReplicationPending();
    }

    @Override
    public void execute(RedisCommand command, ClientConnection conn) throws IOException {
        // for the follower, just execute the command
        byte[] response = command.execute(this);
        if (command.isReplicatedCommand()) {
            System.out.println(
                    String.format("Follower service do not send replicated %s response: %s",
                            command.getType().name(), new String(response)));
            return;
        }
        System.out.println(
                String.format("Follower service command response: %s", new String(response)));
        if (response != null && response.length > 0) {
            conn.writeFlush(response);
        }
    }

    @Override
    public byte[] replicationConfirm(Map<String, RespValue> optionsMap) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            String response = String.format("REPLCONF ACK %d", 0);
            return new RespBulkString(response.getBytes()).asResponse();
        }
        return RespConstants.OK;
    }

}
