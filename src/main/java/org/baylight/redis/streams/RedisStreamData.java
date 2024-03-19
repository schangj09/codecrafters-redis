package org.baylight.redis.streams;

import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.protocol.RespValue;

public class RedisStreamData {
    private final String streamKey;
    private final OrderedArrayList<StreamId> streamIds = new OrderedArrayList<>();
    private final Map<StreamId, RespValue[]> dataValues = new HashMap<>();

    public RedisStreamData(String streamKey) {
        this.streamKey = streamKey;
    }

    public synchronized StreamId add(String itemId, RespValue[] values) throws IllegalStreamItemIdException {
        StreamId streamId;
        String[] ids = itemId.split("-");
        if (ids.length == 2) {
            long timeId = Long.parseLong(ids[0]);
            int counter = Integer.parseInt(ids[1]);
            streamId = new StreamId(timeId, counter);
            if (StreamId.compare(streamId, streamIds.last()) >= 0) {
                throw new IllegalStreamItemIdException(String.format(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"));
            }
            streamIds.add(streamId);
            dataValues.put(streamId, values);
        } else {
            throw new IllegalStreamItemIdException(String.format("ERR: unknown id %s", itemId));
        }
        notifyAll();
        return streamId;
    }

    /**
     * @return the streamKey
     */
    public String getStreamKey() {
        return streamKey;
    }

    /**
     * @return the data for a stream id
     */
    public RespValue[] getData(StreamId id) {
        return dataValues.getOrDefault(id, null);
    }

    @Override
    public String toString() {
        return "RedisStreamData [streamKey=" + streamKey
                + ", lastStreamId=" + streamIds.last()
                + ", dataValues.size=" + dataValues.size() + "]";
    }

}
