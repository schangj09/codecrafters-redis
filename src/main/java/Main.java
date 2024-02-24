import java.io.IOException;
import java.net.ServerSocket;

import org.baylight.redis.EventLoop;

public class Main {
  private static final int PORT = 6379;

  public static void main(String[] args) {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(PORT);
      serverSocket.setReuseAddress(true);
      System.out.println("Server started. Listening on Port " + PORT);
      EventLoop loop = new EventLoop(serverSocket);
      loop.processLoop();
      System.out.println(String.format("Event loop terminated"));

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (InterruptedException e) {
      System.out.println("InterruptedException: " + e.getMessage());
    }
  }
}
