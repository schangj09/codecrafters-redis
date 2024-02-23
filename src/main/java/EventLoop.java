import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventLoop {
    private static final String PONG = "+PONG\r\n";
    private static final String PING = "ping";

    // keep a list of socket connections and continue checking for new connections
    final ServerSocket serverSocket;
    Deque<ClientConnection> clientSockets = new ConcurrentLinkedDeque<>();
    boolean done = false;
    ExecutorService executor;

    public EventLoop(ServerSocket socket) {
        serverSocket = socket;
        executor = Executors.newFixedThreadPool(10); // Adjust thread count as needed

        executor.execute(() -> {
            while (true) {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    ClientConnection conn = new ClientConnection(clientSocket);
                    clientSockets.add(conn);
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                } finally {
                    System.out.println(String.format("Connection: %s, opened: %s", 
                    clientSocket, clientSocket == null ? null : !clientSocket.isClosed()));
                }
            }
        });
    }

    void processLoop() throws InterruptedException {
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
                    while (conn.reader.ready()) {
                        String line = conn.reader.readLine();
                        didProcess = true;
                        process(conn, line);
                    }
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
            // sleep a bit if there were no lines processed
            if (!didProcess) {
                System.out.println("sleep 1s");
                Thread.sleep(1000L);
            }
        }
    }

    void process(ClientConnection conn, String line) throws IOException {
        System.out.println(String.format("Received line: %s", line));
        switch (line) {
            case PING -> {
                conn.writer.write(PONG);
                conn.writer.flush();
            }
            case "EOF" -> {
                conn.clientSocket.close();
            }
            default -> {
                // ignore command line
            }
        }
    }
}
