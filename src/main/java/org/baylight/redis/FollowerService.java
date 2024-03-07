package org.baylight.redis;

import java.io.IOException;
import java.net.Socket;
import java.time.Clock;

import org.baylight.redis.commands.RedisCommand;

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
    public void execute(RedisCommand command, ClientConnection conn) throws IOException {
        // for the follower, just execute the command
        conn.writeFlush(command.execute(this));
    }

}
