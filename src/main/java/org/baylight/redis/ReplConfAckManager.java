package org.baylight.redis;

import java.time.Clock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ReplConfAckManager {
    private Map<Object, Set<ClientConnection>> waitFollowerSets = new ConcurrentHashMap<>();
    private Map<Object, Set<ClientConnection>> ackFollowerSets = new ConcurrentHashMap<>();

    public static ReplConfAckManager INSTANCE = new ReplConfAckManager();

    private ReplConfAckManager() {
    }

    // get noitified that a connection has received a replconf ack
    public void notifyFollowerSet(ClientConnection connection) {
        waitFollowerSets.forEach((lock, connectionSet) -> {
            if (connectionSet.contains(connection)) {
                synchronized (lock) {
                    ackFollowerSets.get(lock).add(connection);
                    lock.notifyAll();
                }
            }
        });
    }

    // wait for a set of connections to receive a replconf ack
    public void waitForFollowerSet(int requestWaitFor, Set<ClientConnection> followerSet,
            Clock clock,
            long timeoutMillis) {
        // create lock for waiting on acks from this set of connections
        Object lock = new Object();
        waitFollowerSets.put(lock, followerSet);
        Set<ClientConnection> ackSet = new HashSet<>();
        ackFollowerSets.put(lock, ackSet);
        synchronized (lock) {
            long start = clock.millis();
            long now = start;
            int numToWaitFor = Math.min(requestWaitFor, followerSet.size());
            try {
                while ((timeoutMillis == 0 || now - start < timeoutMillis)
                        && ackSet.size() < numToWaitFor) {
                    // suspend this synchronized section and wait until the lock is notified that an ack has been received
                    lock.wait(timeoutMillis);
                    now = clock.millis();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(String.format(
                        "waitForFollowerSet: exception while waiting for acks: %s %s",
                        followerSet,
                        this));
            } finally {
                waitFollowerSets.remove(lock);
                ackFollowerSets.remove(lock);
            }
        }
    }

    @Override
    public String toString() {
        return "ReplConfAckManager [waitFollowerSets=" + waitFollowerSets + ", ackFollowerSets="
                + ackFollowerSets + "]";
    }

}
