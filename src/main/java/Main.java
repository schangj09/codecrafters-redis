import java.io.IOException;
import java.time.Clock;

import org.baylight.redis.EventLoop;
import org.baylight.redis.RedisService;
import org.baylight.redis.protocol.RespConstants;

public class Main {
  public static void main(String[] args) {

    int port = RespConstants.DEFAULT_PORT;
    if (args.length > 1 && "--port".equals(args[0])) {
      try {
        port = Integer.parseInt(args[1]);
        if (port <= 0 || port > 65535) {
          throw new IllegalArgumentException("Value must be less than or equal to 65535: " + port);
        }
      } catch (Exception e) {
        System.out.println("Invalid value for --port: " + e.getMessage());
        throw e;
      }
    }
    RedisService service = new RedisService(port, Clock.systemUTC());
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
