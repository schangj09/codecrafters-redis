package org.baylight.redis;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;
import java.util.Map;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.protocol.RespArrayValue;
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
    public void execute(RedisCommand command, ClientConnection conn, boolean writeResponse) throws IOException {
        // for the follower, just execute the command
        byte[] response = command.execute(this);
        if (writeResponse) {
            System.out.println(String.format("Follower service sending GETACK response: %s",
                    new String(response)));
            if (response != null && response.length > 0) {
                conn.writer.writeFlush(response);
            }
        } else {
            System.out.println(String.format("Follower service do not send %s response: %s",
                    command.getType().name(), new String(response)));
        }
    }


    @Override
    public byte[] replicationConfirm(Map<String, RespValue> optionsMap) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            String responseValue = String.valueOf(leaderConnection.getNumBytesReceived());
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(RedisCommand.Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(responseValue.getBytes()) }).asResponse();
        }
        return RespConstants.OK;
    }

    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        return 0;
    }
}
