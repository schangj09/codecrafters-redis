package org.baylight.redis.streams;

import java.util.Objects;

class StreamId {

    private final long timeId;
    private final int counter;

    public StreamId(long timeId, int counter) {
        this.timeId = timeId;
        this.counter = counter;
    }

    /**
     * @return the timeId
     */
    public long getTimeId() {
        return timeId;
    }

    /**
     * @return the counter
     */
    public int getCounter() {
        return counter;
    }

    public String getId() {
        return timeId + "-" + counter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeId, counter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof StreamId))
            return false;
        StreamId other = (StreamId) obj;
        return timeId == other.timeId && counter == other.counter;
    }

    @Override
    public String toString() {
        return "StreamId [timeId=" + timeId + ", counter=" + counter + "]";
    }

    public static int compare(StreamId o1, StreamId o2) {
        return o1.timeId == o2.timeId
                ? Integer.compare(o1.counter, o2.counter)
                : Long.compare(o1.timeId, o2.timeId);
    }

}