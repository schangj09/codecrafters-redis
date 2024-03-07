package org.baylight.redis;

import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import org.baylight.redis.commands.PingCommand;
import org.baylight.redis.commands.PsyncCommand;
import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.ReplConfCommand;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public class ConnectionToLeader {
    // keep a list of socket connections and continue checking for new connections
    private final FollowerService service;
    private final ClientConnection leaderConnection;
    private final Deque<CommandAndResponseConsumer> commandsToLeader = new ConcurrentLinkedDeque<>();
    private final ExecutorService executor;
    private volatile boolean done = false;
    private volatile boolean handshakeComplete = false;
    private RespBulkString fullResyncRdb;

    public ConnectionToLeader(FollowerService service) throws IOException {
        this.service = service;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for sending commands
                                                    // to the leader

        leaderConnection = new ClientConnection(service.getLeaderClientSocket());
        System.out.println(String.format("Connection: %s, isOpened: %s",
                leaderConnection.clientSocket, !leaderConnection.clientSocket.isClosed()));

        // create the thread for sending commands to the leader
        executor.execute(() -> {
            try {
                processLoop();
            } catch (InterruptedException e) {
                System.out
                        .println("InterruptedException on send command thread: " + e.getMessage());
                terminate();
            }
        });
    }

    public boolean isHandshakeComplete() {
        return handshakeComplete;
    }

    public void sendLeaderCommand(RedisCommand command,
            BiFunction<RedisCommand, RespValue, Boolean> responseConsumer) {
        if (!isHandshakeComplete()) {
            throw new RuntimeException("Handshake not complete. Cannot send command to leader.");
        }
        sendCommand(command, responseConsumer);
    }

    public void startHandshake() {
        System.out.println(String.format("Staring handshake with leader"));
        sendCommand(new PingCommand(), (cmd, response) -> {
            ReplConfCommand conf1 = new ReplConfCommand(ReplConfCommand.Option.LISTENING_PORT,
                    String.valueOf(service.getPort()));
            sendCommand(conf1, (conf1Cmd, response2) -> {
                ReplConfCommand conf2 = new ReplConfCommand(ReplConfCommand.Option.CAPA, "psync2");
                sendCommand(conf2, (conf2Cmd, response3) -> {
                    PsyncCommand psync = new PsyncCommand("?", Long.valueOf(-1L));
                    sendCommand(psync, (psyncCmd, response4) -> {
                        if (response4.isSimpleString() && response4.getValueAsString().toUpperCase().startsWith("FULLRESYNC")) {
                            System.out.println(String.format("Full resync - looking for rdb response"));
                            return true;
                        }
                        if (!response4.isBulkString()) {
                            throw new RuntimeException(String.format("Unexpected response: %s", response4));
                        }
                        setFullResyncRdb((RespBulkString) response4);
                        System.out.println(String.format("Handshake completed"));
                        handshakeComplete = true;
                        return false;
                    });
                    return false;
                });
                return false;
            });
            return false;
        });
    }

    private void setFullResyncRdb(RespBulkString fullResyncRdb) {
        this.fullResyncRdb = fullResyncRdb;
    }
    public byte[] getFullResyncRdb() {
        return fullResyncRdb.getValue();
    }
    private void sendCommand(RedisCommand command,
            BiFunction<RedisCommand, RespValue, Boolean> responseConsumer) {
        CommandAndResponseConsumer cmd = new CommandAndResponseConsumer(command, responseConsumer);
        // add the command to the queue
        commandsToLeader.offerLast(cmd);
    }

    public void terminate() {
        System.out.println(String.format("Terminate follower invoked. Closing socket to leader %s.",
                service.getLeaderClientSocket()));
        done = true;
        // close the connection to the leader
        try {
            service.getLeaderClientSocket().close();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        // executor close - waits for thread to finish
        executor.close();
    }

    public void processLoop() throws InterruptedException {
        while (!done) {
            // check for commands waiting to be sent
            boolean didProcess = false;

            if (leaderConnection.clientSocket.isClosed()) {
                System.out.println(
                        String.format("Connection closed: %s", leaderConnection.clientSocket));
                terminate();
                continue;
            }

            try {
                while (!commandsToLeader.isEmpty()) {
                    CommandAndResponseConsumer cmd = commandsToLeader.pollFirst();
                    // send the command to the leader
                    System.out.println(String.format("Sending leader command: %s", cmd.command));
                    leaderConnection.writer.write(cmd.command.asCommand());
                    leaderConnection.writer.flush();

                    // read the response - will wait on the stream until the whole value is parsed
                    RespValueParser respValueParser = new RespValueParser();
                    RespValue response;
                    // responseConsumer returns True if we expect another value from the command
                    do {
                        response = respValueParser.parse(leaderConnection.reader);
                        System.out.println(String.format("Received leader response: %s", response));
                    } while (cmd.responseConsumer.apply(cmd.command, response));

                    didProcess = true;
                }
            } catch (Exception e) {
                System.out.println(String.format("Exception: %s \"%s\"",
                        e.getClass().getSimpleName(), e.getMessage()));
            }
            // sleep a bit if there were no lines processed
            if (!didProcess) {
                // System.out.println("sleep 1s");
                Thread.sleep(500L);
            }
        }
    }

    private static class CommandAndResponseConsumer {
        private final RedisCommand command;
        private final BiFunction<RedisCommand, RespValue, Boolean> responseConsumer;

        public CommandAndResponseConsumer(RedisCommand command,
                BiFunction<RedisCommand, RespValue, Boolean> responseConsumer) {
            this.command = command;
            this.responseConsumer = responseConsumer;
        }
    }
}
