package org.baylight.redis;

import java.io.IOException;
import java.time.Clock;

public class ServiceRunner implements Runnable {
    RedisServiceOptions options;
    RedisServiceBase service = null;

    public ServiceRunner(String... args) {
        options = new RedisServiceOptions();
        if (!options.parseArgs(args)) {
            throw new RuntimeException("Invalid arguments");
        }
    }

    public ServiceRunner(RedisServiceOptions options) {
        this.options = options;
    }

    public void terminate() {
        if (service != null) {
            service.terminate();
        }
    }

    public void run() {
        service = RedisServiceBase.newInstance(options, Clock.systemUTC());
        try {
            service.start();
            service.runCommandLoop();
            System.out.println(String.format("Event loop terminated"));

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } catch (InterruptedException e) {
            System.out.println("InterruptedException: " + e.getMessage());
        } finally {
            service.shutdown();
            service = null;
        }
    }

}
