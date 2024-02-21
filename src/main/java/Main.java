import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
      BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      boolean isEof = false;
      while (!isEof) {
        String line = reader.readLine();
        System.out.println(String.format("Received line: %s", line));
        switch (line) {
          case "ping" -> {
            writer.write("+POMG\r\n");
            writer.flush();
          }
          case "EOF" -> {
            isEof = true;
          }
          default -> {
            // ignore command line
          }
        }
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      System.out.println(String.format("Connection: %s", clientSocket));
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }
}
