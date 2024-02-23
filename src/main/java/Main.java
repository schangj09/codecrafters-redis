import java.io.IOException;
import java.net.ServerSocket;

public class Main {
  private static final int PORT = 6379;

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(PORT);
      serverSocket.setReuseAddress(true);
      System.out.println("Server started. Listening on Port " + PORT);
      EventLoop loop = new EventLoop(serverSocket);
      loop.processLoop();

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (InterruptedException e) {
      System.out.println("InterruptedException: " + e.getMessage());
    } finally {
      System.out.println(String.format("All connections closed"));
    }
  }
}
