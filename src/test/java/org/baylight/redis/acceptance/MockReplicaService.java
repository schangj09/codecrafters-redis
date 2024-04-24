package org.baylight.redis.acceptance;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.baylight.redis.ClientConnection;
import org.baylight.redis.commands.PingCommand;
import org.baylight.redis.commands.PsyncCommand;
import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommandConstructor;
import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public class MockReplicaService extends Thread {
    ClientConnection leader;
    List<CommandResponse> expectedResponses = new ArrayList<>();
    List<Exception> exceptions = new ArrayList<>();
    int nextCommand = -1;
    RedisCommandConstructor constructor = new RedisCommandConstructor();

    private void log(String message) {
        System.out.println("MockReplicaService: " + message);
    }

    public MockReplicaService(int leaderPort)
            throws UnknownHostException, IOException {
        leader = new ClientConnection(new Socket("localhost", leaderPort), new RespValueParser());
        // send the handshake commands and then start a thread to read commands from the leader
        log("Sending handshake to leader " + leaderPort);
        leader.writeFlush(new PingCommand().asCommand());
        RespValue value;
        value = leader.readValue();
        log("Received PING response from leader: " + value);

        ReplConfCommand conf1 = new ReplConfCommand(ReplConfCommand.Option.LISTENING_PORT,
                String.valueOf(leaderPort));
        leader.writeFlush(conf1.asCommand());
        value = leader.readValue();
        log("Received REPLCONF response from leader: " + value);

        ReplConfCommand conf2 = new ReplConfCommand(ReplConfCommand.Option.CAPA,
                "psync2");
        leader.writeFlush(conf2.asCommand());
        value = leader.readValue();
        log("Received REPLCONF CAPA response from leader: " + value);

        PsyncCommand psync = new PsyncCommand("?", Long.valueOf(-1L));
        leader.writeFlush(psync.asCommand());
        value = leader.readValue();
        log("Received PSYNC response from leader: " + value);

        byte[] rdb = leader.readRDB();
        log("Received RDB bytes size: " + rdb.length);
    }

    public void expect(CommandResponse expectedCommand) {
        expectedResponses.add(expectedCommand);
        if (nextCommand < 0) {
            nextCommand = 0;
        }
    }

    @Override
    public void run() {
        while (nextCommand < expectedResponses.size()) {
            try {
                if (leader.available() > 0) {
                    RespValue value = leader.readValue();
                    if (nextCommand >= 0) {
                        log("Received value: " + value);
                        CommandResponse expectedResponse = expectedResponses.get(nextCommand);
                        RedisCommand actualCommand = constructor.newCommandFromValue(value);
                        if (expectedResponse.commandMatches(actualCommand)) {
                            nextCommand++;
                            if (expectedResponse.delayMillis != null) {
                                log(String.format("Waiting %d millis",
                                        expectedResponse.delayMillis));
                                Thread.sleep(expectedResponse.delayMillis);
                            }
                            if (expectedResponse.response != null) {
                                log(String.format("Sending response %s",
                                        expectedResponse.response));
                                leader.writeFlush(expectedResponse.response.asResponse());
                            }
                        } else {
                            throw new RuntimeException(
                                    String.format("Expected: %s but got %s for command %d",
                                            expectedResponse.command, actualCommand, nextCommand));
                        }
                    } else {
                        throw new RuntimeException("Unexpected value: " + value);
                    }
                }

                Thread.sleep(100L);
            } catch (Exception e) {
                e.printStackTrace();
                exceptions.add(e);
            }
        }
    }

    public void terminate() {
        if (nextCommand < expectedResponses.size()) {
            log(String.format("Terminating mock replica service with %d expected commands",
                    expectedResponses.size() - nextCommand));
            nextCommand = expectedResponses.size();
        }
        // shutdown the thread
        this.interrupt();
    }

}
