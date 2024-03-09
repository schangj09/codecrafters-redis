package org.baylight.redis;

import java.io.IOException;
import java.time.Clock;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.baylight.redis.commands.PingCommand;
import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommand.Type;
import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.commands.SetCommand;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public class LeaderService extends RedisServiceBase {
    private final static String EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    long totalReplicationOffset = 0L;
    Map<String, Long> replicationOffsets = new HashMap<>();
    Map<String, ConnectionToFollower> replMap = new ConcurrentHashMap<>();
    boolean isDebugMode = false;

    public LeaderService(RedisServiceOptions options, Clock clock) {
        super(options, clock);
    }

    long getFollowerOffset(String follower) {
        return replicationOffsets.getOrDefault(follower, 0L);
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getTotalReplicationOffset() {
        return totalReplicationOffset;
    }

    @Override
    public void execute(RedisCommand command, ClientConnection conn, boolean writeResponse)
            throws IOException {
        // for the leader, return the command response and replicate to the followers
        byte[] response = command.execute(this);
        // Note: first complete replication before sending the response

        // check if it is a new follower
        String connectionString = conn.getConnectionString();
        if (command.getType() == Type.REPLCONF && !replMap.containsKey(connectionString)) {
            // if so, save it as a follower
            replMap.put(connectionString, new ConnectionToFollower(this, conn));
        }
        // replicate to the followers
        if (command.isReplicatedCommand()) {
            Iterator<ConnectionToFollower> iter = replMap.values().iterator();
            while (iter.hasNext()) {
                ConnectionToFollower follower = iter.next();
                ClientConnection clientConnection = follower.getFollowerConnection();
                if (clientConnection.isClosed()) {
                    System.out.println(
                            String.format("Follower connection closed: %s", conn.clientSocket));
                    iter.remove();
                    continue;
                }
                if (clientConnection != conn) {
                    try {
                        clientConnection.writer.writeFlush(command.asCommand());

                        // for set command do a PING and GETACK
                        if (isDebugMode && command instanceof SetCommand) {
                            PingCommand ping = new PingCommand();
                            clientConnection.writer.writeFlush(ping.asCommand());
                            // leader does not respond to ping, but it will count the bytes for the ack response
                            System.out.println(String.format("Debug ping call succeeded."));

                            ReplConfCommand ack = new ReplConfCommand(ReplConfCommand.Option.GETACK,
                                    "*");
                            clientConnection.writer.writeFlush(ack.asCommand());
                            RespValue ackResponse = new RespValueParser()
                                    .parse(clientConnection.reader);
                            System.out.println(String.format("Debug ack call response: %s",
                                    ackResponse.toString()));
                        }
                    } catch (IOException e) {
                        System.out.println(String.format(
                                "Follower exception during replication connection: %s, exception: %s",
                                conn.clientSocket, e.getMessage()));
                    }
                }
            }
        }
        if (response != null && response.length > 0 && writeResponse) {
            conn.writer.writeFlush(response);
        }
    }

    @Override
    public void getReplcationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }

    @Override
    public byte[] replicationConfirm(Map<String, RespValue> optionsMap) {
        if (optionsMap.containsKey(ReplConfCommand.GETACK_NAME)) {
            String responseValue = String.valueOf(totalReplicationOffset);
            return new RespArrayValue(new RespValue[] {
                    new RespBulkString(RedisCommand.Type.REPLCONF.name().getBytes()),
                    new RespBulkString("ACK".getBytes()),
                    new RespBulkString(responseValue.getBytes()) }).asResponse();
        }
        return RespConstants.OK;
    }

    @Override
    public int waitForReplicationServers(int numReplicas, long timeoutMillis) {
        // Note: should return all replicated services even if greater than requested number
        int count = replMap.size();
        return count;
    }

    @Override
    public byte[] psync(Map<String, RespValue> optionsMap) {
        String response = String.format("FULLRESYNC %s %d", replicationId, totalReplicationOffset);
        return new RespSimpleStringValue(response).asResponse();
    }

    @Override
    public byte[] psyncRdb(Map<String, RespValue> optionsMap) {
        byte[] rdbData = Base64.getDecoder().decode(EMPTY_RDB_BASE64);
        return new RespBulkString(rdbData).asResponse(false);
    }
}
