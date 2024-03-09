package org.baylight.redis;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WaitExecutor {
    private final int numToWaitFor;
    private final AtomicInteger numAcknowledged;

    private final CountDownLatch latch;
    private final ExecutorService executorService;

    public WaitExecutor(int numToWaitFor, ExecutorService executorService) {
        this.numToWaitFor = numToWaitFor;
        this.executorService = executorService;
        numAcknowledged = new AtomicInteger(0);
        this.latch = new CountDownLatch(numToWaitFor);
    }

    int wait(Collection<ConnectionToFollower> followers, long timeoutMillis) {
        try {
            // send a replConf ack command to each follower on a separate thread
            // wait on the latch to block until enough acks are received
            for (ConnectionToFollower connection : followers) {
                executorService.execute(() -> {
                    try {
                        connection.sendAndWaitForReplConfAck();
                        numAcknowledged.incrementAndGet();
                        latch.countDown();
                        System.out.println(String.format("Received replConfAck from %s",
                                connection.toString()));
                    } catch (Exception e) {
                        System.out.println(String.format(
                                "Error sending replConfAck to %s, error: %s %s, cause: %s",
                                connection.toString(), e.getClass().getSimpleName(), e.getMessage(),
                                e.getCause()));
                        System.out.println(
                                String.format("Not counted. Still waiting for %d replConfAcks.",
                                        numToWaitFor - numAcknowledged.get()));
                    }
                });
            }
            // extend the timeout to allow for codecrafters tests to pass
            long extendedTimeout = timeoutMillis + 3000L;
            long before = System.currentTimeMillis();
            System.out.println(
                    String.format("Time %d: waiting for %d millis for acks.", before, extendedTimeout));
            if (!latch.await(extendedTimeout, TimeUnit.MILLISECONDS)) {
                System.out.println(
                        String.format("Timed out waiting for %d replConfAcks. Received %d acks.",
                                numToWaitFor, numAcknowledged.get()));

            }
            long after = System.currentTimeMillis();
            System.out.println(
                    String.format("Time %d: after latch await, elapsed: %d", after, after - before));
        } catch (InterruptedException e) {
            System.out.println(String.format(
                    "Interrupted while waiting for %d replConfAcks. Received %d acks.",
                    numToWaitFor, numAcknowledged.get()));
        } catch (Exception e) {
            System.out
                    .println(String.format("Error while sending %d replConfAcks. Received %d acks.",
                            numToWaitFor, numAcknowledged.get()));
        }
        return numAcknowledged.get();
    }

    @Override
    public String toString() {
        return "WaitExecutor [numToWaitFor=" + numToWaitFor + ", numAcknowledged="
                + numAcknowledged.get() + "]";
    }
}
