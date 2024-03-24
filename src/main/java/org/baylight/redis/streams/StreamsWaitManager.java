package org.baylight.redis.streams;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class StreamsWaitManager {
    private Map<Object, Set<String>> waitStreamSets = new ConcurrentHashMap<>();

    public static StreamsWaitManager INSTANCE = new StreamsWaitManager();

    private StreamsWaitManager() {
    }

    public void addNotify(String streamKey) {
        waitStreamSets.forEach((lock, streamSet) -> {
            if (streamSet.contains(streamKey)) {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
    }

    public Map<String, List<StreamValue>> readWithWait(
            Map<String, RedisStreamData> streams, Map<String, StreamId> startIds, int count,
            Clock clock, long timeoutMillis) {
        Map<String, List<StreamValue>> result = new HashMap<>();
        int countPerStream = count;
        // if count not specified, then we try to read max number of values from each stream,
        // but we only need 1 value to end waiting
        if (count == 0) {
            count = 1;
            countPerStream = RedisStreamData.MAX_READ_COUNT;
        }

        // create lock for waiting on the set of streams
        Object lock = new Object();
        waitStreamSets.put(lock, streams.keySet());
        synchronized (lock) {

            long start = clock.millis();
            long now = start;
            try {
                int readCount = 0;
                while ((timeoutMillis == 0 || now - start < timeoutMillis)
                 && readCount < count) {

                    // read from each stream and wait if not enough data
                    for (String streamKey : streams.keySet()) {
                        StreamId startId = startIds.get(streamKey);
                        List<StreamValue> nextValues = streams.get(streamKey).readNextValues(
                                countPerStream, startId);
                        List<StreamValue> values = result.computeIfAbsent(streamKey,
                                k -> new ArrayList<>());
                        values.addAll(nextValues);
                        readCount += nextValues.size();
                    }
                    if (readCount >= count) {
                        return result;
                    }
                    countPerStream = count - readCount;
                    // wait until notified that one of the streams has more data
                    lock.wait(timeoutMillis);
                    now = clock.millis();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(String.format(
                        "readWithWait: exception while reading from streams: %s %s",
                        streams.keySet(),
                        this));
            } finally {
                // clean up the lock now that we are done waiting
                waitStreamSets.remove(lock);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "StreamsWaitManager [waitStreamSets=" + waitStreamSets + "]";
    }

}
