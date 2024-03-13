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
    private final RespValueParser valueParser;
    private volatile boolean done = false;
    private volatile boolean handshakeComplete = false;
    private volatile boolean replicationPending = true;
    private long startBytesOffset = 0;
    private RespBulkString fullResyncRdb;

    public ConnectionToLeader(FollowerService service) throws IOException {
        this.service = service;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for sending commands
                                                    // to the leader
        valueParser = new RespValueParser();

        leaderConnection = new ClientConnection(service.getLeaderClientSocket(), valueParser);
        System.out.println(String.format("Connection to leader: %s, isOpened: %s", leaderConnection,
                !leaderConnection.isClosed()));

        // create the thread for sending commands to the leader and receiving replication commands
        executor.execute(() -> {
            try {
                runHandshakeLoop();
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

    public long getNumBytesReceived() {
        return leaderConnection.getNumBytesReceived() - startBytesOffset;
    }

    public void sendLeaderCommand(RedisCommand command,
            BiFunction<RedisCommand, RespValue, Boolean> responseConsumer) {
        if (!isHandshakeComplete()) {
            throw new RuntimeException("Handshake not complete. Cannot send command to leader.");
        }
        sendCommand(command, responseConsumer);
    }

    public void startHandshake() {
        System.out.println(String.format("Starting handshake with leader"));
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
                        startBytesOffset = leaderConnection.getNumBytesReceived();
                        handshakeComplete = true;

                        // after the handshake, allow the ConnectionManager to poll for commands
                        // from the leader and process them in the FollowerService on the main event
                        // loop
                        service.getConnectionManager().addPriorityConnection(leaderConnection);
                        replicationPending = false;
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

    public void runHandshakeLoop() throws InterruptedException {
        while (!done && !handshakeComplete) {
            // check for commands waiting to be sent
            boolean didProcess = false;

            if (leaderConnection.isClosed()) {
                System.out.println(String.format(
                        "Terminating process due to connection is closed by leader: %s",
                        leaderConnection));
                terminate();
                continue;
            }

            try {
                while (!commandsToLeader.isEmpty()) {
                    CommandAndResponseConsumer cmd = commandsToLeader.pollFirst();
                    // send the command to the leader
                    System.out.println(String.format("Sending leader command: %s", cmd.command));
                    leaderConnection.writeFlush(cmd.command.asCommand());

                    // read the response - will wait on the stream until the whole value is parsed
                    RespValue response = leaderConnection.readValue();
                    System.out.println(String.format("Received leader response: %s", response));

                    // responseConsumer returns True if we expect the RDB value from the command
                    if (cmd.responseConsumer.apply(cmd.command, response)) {
                        try {
                            byte[] rdb = leaderConnection.readRDB();

                            response = new RespBulkString(rdb);
                            System.out.println(String.format("Received leader RDB: %s", response));
                            cmd.responseConsumer.apply(cmd.command, response);
                        } catch (IOException e) {
                            System.out.println(String.format(
                                    "ConnectionToLeader: IOException on readRDB: %s %s",
                                    e.getClass().getSimpleName(), e.getMessage()));
                        }
                    }
                }
                // sleep a bit if there were no commands processed
                // Note: handshake does not count so we will sleep after the handshake
                if (!didProcess) {
                    // System.out.println("sleep 1s");
                    Thread.sleep(50L);
                }
            } catch (Exception e) {
                System.out.println(String.format("ConnectionToLeader Loop Exception: %s \"%s\"",
                        e.getClass().getSimpleName(), e.getMessage()));
            }
        }
        System.out.println(String.format(
                "Exiting thread for handshake commands - done: %s, handshakeComplete: %s", done,
                handshakeComplete));
    }

    public void executeCommand(ClientConnection conn, RedisCommand command) throws IOException {
        if (leaderConnection.equals(conn)) {
            if (command.isReplicatedCommand()) {
                System.out.println(
                        String.format("Received replicated command from leader: %s", conn));
            } else {
                System.out.println(String.format("Received request from leader: %s", conn));
            }
        }

        try {
            // TODO remove replicationPending
            // replicationPending should be unnecessary now that we are processing commands
            // from the leader (both replication and getack) on the main service loop and we
            // have ensured the replication commands get processed first before other connections
            replicationPending = true;
            // if the command came from the leader, then for most commands the leader does not
            // expect a response
            boolean writeResponse = shouldSendResponseToConnection(command, conn);

            byte[] response = command.execute(service);
            if (writeResponse) {
                System.out.println(String.format("Follower service sending %s response: %s",
                        command.getType().name(), RedisCommand.responseLogString(response)));
                if (response != null && response.length > 0) {
                    conn.writeFlush(response);
                }
            } else {
                System.out.println(String.format("Follower service do not send %s response: %s",
                        command.getType().name(), RedisCommand.responseLogString(response)));
            }
        } finally {
            replicationPending = false;
        }
    }

    public boolean shouldSendResponseToConnection(RedisCommand command, ClientConnection conn) {
        if (!leaderConnection.equals(conn)) {
            return true;
        }
        boolean isReplconfGetack = command instanceof ReplConfCommand && ((ReplConfCommand) command)
                .getOptionsMap().containsKey(ReplConfCommand.GETACK_NAME);
        return isReplconfGetack;
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
