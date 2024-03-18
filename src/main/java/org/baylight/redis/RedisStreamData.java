package org.baylight.redis;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.baylight.redis.protocol.RespValue;

public class RedisStreamData {
    private final String streamKey;
    private final Queue<Map<String, RespValue>> dataValues;

    public RedisStreamData(String streamKey) {
        this.streamKey = streamKey;
        this.dataValues = new ConcurrentLinkedQueue<>();
    }

    void add(Map<String, RespValue> values) {
        dataValues.offer(values);
    }

    /**
     * @return the streamKey
     */
    public String getStreamKey() {
        return streamKey;
    }

    /**
     * @return the dataValues
     */
    public Queue<Map<String, RespValue>> getDataValues() {
        return dataValues;
    }

    @Override
    public String toString() {
        return "RedisStreamData [streamKey=" + streamKey
                + ", dataValues.size=" + dataValues.size() + "]";
    }

}
