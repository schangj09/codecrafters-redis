package org.baylight.redis;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.baylight.redis.protocol.RespValue;

public class RedisStreamData {
    private final String streamKey;
    private final Queue<String> itemIds;
    private final Map<String, Map<String, RespValue>> dataValues;

    public RedisStreamData(String streamKey) {
        this.streamKey = streamKey;
        this.itemIds = new ConcurrentLinkedQueue<>();
        this.dataValues = new ConcurrentHashMap<>();
    }

    void add(String itemId, Map<String, RespValue> values) {
        itemIds.offer(itemId);
        dataValues.put(itemId, values);
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
    public Queue<String> getItemIds() {
        return itemIds;
    }

    @Override
    public String toString() {
        return "RedisStreamData [streamKey=" + streamKey
                + ", dataValues.size=" + dataValues.size() + "]";
    }

}
