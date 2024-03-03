import java.io.IOException;
import java.time.Clock;

import org.baylight.redis.EventLoop;
import org.baylight.redis.RedisService;
import org.baylight.redis.RedisServiceOptions;

public class Main {
  public static void main(String[] args) {

    RedisServiceOptions options = new RedisServiceOptions();
    if (!options.parseArgs(args)) {
      System.out.println("Invalid arguments");
      return;
    }

    RedisService service = new RedisService(options, Clock.systemUTC());
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
