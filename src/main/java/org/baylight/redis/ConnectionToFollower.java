package org.baylight.redis;

import java.io.IOException;

import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public class ConnectionToFollower {
    private final LeaderService service;
    private final ClientConnection followerConnection;
    private volatile boolean handshakeComplete = false;

    public ConnectionToFollower(LeaderService service, ClientConnection followerConnection)
            throws IOException {
        this.service = service;
        this.followerConnection = followerConnection;
    }

    public long getTotalReplicationOffset() {
        return service.getTotalReplicationOffset();
    }

    public boolean isHandshakeComplete() {
        return handshakeComplete;
    }

    public void setHandshakeComplete() {
        this.handshakeComplete = true;
    }

    public ClientConnection getFollowerConnection() {
        return followerConnection;
    }

    public RespValue sendAndWaitForReplConfAck() throws IOException {
        ReplConfCommand ack = new ReplConfCommand(ReplConfCommand.Option.GETACK, "*");
        followerConnection.writer.writeFlush(ack.asCommand());
        return new RespValueParser().parse(followerConnection.reader);
    }

    @Override
    public String toString() {
        return "ConnectionToFollower: " + followerConnection.clientSocket;
    }

}
