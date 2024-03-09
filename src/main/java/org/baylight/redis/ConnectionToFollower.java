package org.baylight.redis;

import java.io.IOException;

import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespType;
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
        System.out.println(String.format("sendAndWaitForReplConfAck: Sending command %s",
                new String(ack.asCommand())));
        followerConnection.writer.writeFlush(ack.asCommand());

        BufferedInputLineReader reader = followerConnection.reader;
        System.out.println(
                String.format("sendAndWaitForReplConfAck: Waiting for ACK from %s, bytes read: %d",
                        followerConnection.clientSocket, reader.getNumBytesReceived()));

        int type = reader.read();
        RespType respType = RespType.of((char) type);
        System.out.println(String.format(
                "sendAndWaitForReplConfAck: Waiting for ACK from %s, type %s, bytes read: %d",
                followerConnection.clientSocket, respType.name(), reader.getNumBytesReceived()));

        return switch (respType) {
        case SIMPLE_STRING -> new RespSimpleStringValue(reader);
        // case SIMPLE_ERROR -> new SimpleErrorRespValue(reader);
        case INTEGER -> new RespInteger(reader);
        case BULK_STRING -> new RespBulkString(reader);
        case ARRAY -> new RespArrayValue(reader, new RespValueParser());
        case null, default -> {
            System.out.println("Unknown type: " + type);
            yield null;
        }
        };

    }

    @Override
    public String toString() {
        return "ConnectionToFollower: " + followerConnection.clientSocket;
    }

}
