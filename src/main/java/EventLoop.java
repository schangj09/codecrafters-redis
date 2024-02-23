import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventLoop {
    private static final String TERMINATE = "terminate";
    private static final String EOF = "EOF";
    private static final String PONG = "+PONG\r\n";
    private static final String PING = "ping";

    // keep a list of socket connections and continue checking for new connections
    private final ServerSocket serverSocket;
    private final Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    private final ExecutorService executor;
    private volatile boolean done = false;

    public EventLoop(ServerSocket socket) {
        serverSocket = socket;
        executor = Executors.newFixedThreadPool(1); // We need just one thread for accepting new connections

        // create the thread for accepting new connections
        executor.execute(() -> {
            while (!done) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    ClientConnection conn = new ClientConnection(clientSocket);
                    clientSockets.add(conn);
                    System.out.println(
                            String.format("Connection: %s, opened: %s", clientSocket, !clientSocket.isClosed()));
                } catch (IOException e) {
                    System.out.println("IOException on accept: " + e.getMessage());
                }
            }

            // loop was terminated so close any open connections
            for (ClientConnection conn : clientSockets) {
                try {
                    System.out.println(
                            String.format(
                                    "Closing connection: %s, opened: %s",
                                    conn.clientSocket,
                                    !conn.clientSocket.isClosed()));
                    if (!conn.clientSocket.isClosed()) {
                        conn.clientSocket.close();
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        });
    }

    public void terminate() {
        System.out.println(String.format("Terminate invoked. Closing %d connections.", clientSockets.size()));
        done = true;
        // stop accepting new connections and shut down the accept connections thread
        try {
            serverSocket.close();
        } catch (IOException e) {
            System.out.println("IOException on socket close: " + e.getMessage());
        }
        // executor close - waits for thread to finish closing all open connections
        executor.close();
    }

    public void processLoop() throws InterruptedException {
        while (!done) {
            // check for bytes on next socket and process
            boolean didProcess = false;
            Iterator<ClientConnection> iter = clientSockets.iterator();
            for (; iter.hasNext();) {
                ClientConnection conn = iter.next();
                if (conn.clientSocket.isClosed()) {
                    System.out.println(String.format("Connection closed: %s", conn.clientSocket));
                    iter.remove();
                    continue;
                }

                try {
                    while (conn.reader.available() > 0) {
                        RedisCommand command = RedisCommand.parseCommand(conn.reader);
                        didProcess = true;
                        if (command != null) {
                            process(conn, command);
                        }
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
            // sleep a bit if there were no lines processed
            if (!didProcess) {
                // System.out.println("sleep 1s");
                Thread.sleep(1000L);
            }
        }
    }

    void process(ClientConnection conn, RedisCommand command) throws IOException {
        System.out.println(String.format("Received line: %s", command));

        byte[] response = command.getResponse();
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
}
