package org.baylight.redis;

import java.io.IOException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import org.baylight.redis.commands.PingCommand;
import org.baylight.redis.commands.RedisCommand;
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
    public void sendLeaderCommand(RedisCommand command, BiConsumer<RedisCommand, RespValue> responseConsumer) {
        if (!isHandshakeComplete()) {
            throw new RuntimeException("Handshake not complete. Cannot send command to leader.");
        }
        sendCommand(command, responseConsumer);
    }

    public void startHandshake() {
        sendCommand(new PingCommand(), (cmd, response) -> {
            System.out.println(String.format("Handshake with leader complete: %s", response));
            handshakeComplete = true;
        });
    }

    private void sendCommand(RedisCommand command, BiConsumer<RedisCommand, RespValue> responseConsumer) {
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
            // check for bytes on next socket and process
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
                    RespValue response = respValueParser.parse(leaderConnection.reader);
                    System.out.println(String.format("Received leader response: %s", response));
                    cmd.responseConsumer.accept(cmd.command, response);

                    didProcess = true;
                }
            } catch (Exception e) {
                System.out.println(String.format("Exception: %s \"%s\"",
                        e.getClass().getSimpleName(), e.getMessage()));
            }
            // sleep a bit if there were no lines processed
            if (!didProcess) {
                // System.out.println("sleep 1s");
                Thread.sleep(80L);
            }
        }
    }

    void process(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received line: %s", command));

        byte[] response = service.execute(command);
        if (response != null) {
            conn.writer.write(response);
            conn.writer.flush();
        }
        switch (command) {
        case EofCommand c -> {
            conn.clientSocket.close();
        }
        case TerminateCommand c -> {
            terminate();
        }
        default -> {
            // no action for other command types
        }
        }
    }

    private static class CommandAndResponseConsumer {
        private final RedisCommand command;
        private final BiConsumer<RedisCommand, RespValue> responseConsumer;

        public CommandAndResponseConsumer(RedisCommand command,
                BiConsumer<RedisCommand, RespValue> responseConsumer) {
            this.command = command;
            this.responseConsumer = responseConsumer;
        }
    }
}
