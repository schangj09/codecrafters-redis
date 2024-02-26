import java.io.IOException;
import java.net.ServerSocket;

import org.baylight.redis.EventLoop;
import org.baylight.redis.RedisService;

public class Main {
  public static void main(String[] args) {
    RedisService service = new RedisService();
    try {
      service.start();
      
      EventLoop loop = new EventLoop(service);
      loop.processLoop();
      System.out.println(String.format("Event loop terminated"));

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } catch (InterruptedException e) {
      System.out.println("InterruptedException: " + e.getMessage());
    } finally {
      service.shutdown();
    }
  }
}
