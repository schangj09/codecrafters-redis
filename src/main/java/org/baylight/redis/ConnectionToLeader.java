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
import org.baylight.redis.commands.RedisCommandConstructor;
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
    private final RedisCommandConstructor commandConstructor;
    private final RespValueParser valueParser;
    private volatile boolean done = false;
    private volatile boolean handshakeComplete = false;
    private volatile boolean replicationPending = true;
    private RespBulkString fullResyncRdb;

    public ConnectionToLeader(FollowerService service) throws IOException {
        this.service = service;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for sending commands
                                                    // to the leader
        commandConstructor = new RedisCommandConstructor();
        valueParser = new RespValueParser();

        leaderConnection = new ClientConnection(service.getLeaderClientSocket());
        System.out.println(String.format("Connection to leader: %s, isOpened: %s",
                leaderConnection.clientSocket, !leaderConnection.clientSocket.isClosed()));

        // create the thread for sending commands to the leader and receiving replication commands
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
    public boolean isReplicationPending() {
        return replicationPending;
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
                        if (response4.isSimpleString() && response4.getValueAsString().toUpperCase()
                                .startsWith("FULLRESYNC")) {
                            System.out.println(
                                    String.format("Full resync - looking for rdb response"));
                            return true;
                        }
                        if (!response4.isBulkString()) {
                            throw new RuntimeException(
                                    String.format("Unexpected response: %s", response4));
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
                System.out.println(String.format(
                        "Terminating process due to connection is closed by leader: %s",
                        leaderConnection.clientSocket));
                terminate();
                continue;
            }

            try {
                // if handshake is completed then read replicated commands from the leader
                if (isHandshakeComplete()) {
                    while (leaderConnection.reader.available() > 0) {
                        replicationPending = true;
                        RedisCommand command = commandConstructor
                                .newCommandFromValue(valueParser.parse(leaderConnection.reader));
                        didProcess = true;
                        if (command != null) {
                            process(leaderConnection, command);
                        }
                    }
                }
                while (!commandsToLeader.isEmpty()) {
                    CommandAndResponseConsumer cmd = commandsToLeader.pollFirst();
                    // send the command to the leader
                    System.out.println(String.format("Sending leader command: %s", cmd.command));
                    leaderConnection.writer.write(cmd.command.asCommand());
                    leaderConnection.writer.flush();

                    // read the response - will wait on the stream until the whole value is parsed
                    RespValueParser respValueParser = new RespValueParser();
                    RespValue response;
                    response = respValueParser.parse(leaderConnection.reader);
                    System.out.println(String.format("Received leader response: %s", response));
                    // responseConsumer returns True if we expect the RDB value from the command
                    if (cmd.responseConsumer.apply(cmd.command, response)) {
                        int val = leaderConnection.reader.read();
                        if (val != '$') {
                            throw new IllegalArgumentException(
                                    "Expected RDB from leader, got char " + val);
                        }
                        long len = leaderConnection.reader.readLong();
                        // TODO: refactor the RDB processing with a stream instead of byte array
                        byte[] rdb = new byte[(int) len];
                        for (int i = 0; i < len; i++) {
                            rdb[i] = (byte) leaderConnection.reader.read();
                        }
                        response = new RespBulkString(rdb);
                        System.out.println(String.format("Received leader RDB: %s", response));
                        cmd.responseConsumer.apply(cmd.command, response);
                    }
                }
                // sleep a bit if there were no commands processed
                // Note: handshake does not count so we will sleep after the handshake
                if (!didProcess) {
                    // System.out.println("sleep 1s");
                    Thread.sleep(50L);
                    // no more replication pending if there is nothing on the socket
                    replicationPending = leaderConnection.reader.available() > 0;
                }
            } catch (Exception e) {
                System.out.println(String.format("ConnectionToLeader Loop Exception: %s \"%s\"",
                        e.getClass().getSimpleName(), e.getMessage()));
            }
        }
    }

    void process(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received replicated command: %s", command));

        service.execute(command, conn);
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
