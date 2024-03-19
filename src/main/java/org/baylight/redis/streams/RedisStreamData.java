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

    public synchronized StreamId add(String itemId, RespValue[] values)
            throws IllegalStreamItemIdException {
        StreamId streamId;
        String[] ids = itemId.split("-");
        if (ids.length == 2) {
            long timeId = 0L;
            try {
                timeId = Long.parseLong(ids[0]);
            } catch (NumberFormatException e) {
                throw new IllegalStreamItemIdException(
                        String.format("ERR: bad id format: %s", itemId));
            }
            int counter = 0;
            try {
                counter = Integer.parseInt(ids[1]);
            } catch (NumberFormatException e) {
                if (!"*".equals(ids[1])) {
                    throw new IllegalStreamItemIdException(
                            String.format("ERR: bad id format: %s", itemId));
                }
                // if timeId not seen yet, then counter remains 0, except for special case
                if (streamIds.size() == 0 || streamIds.last().getTimeId() != timeId) {
                    // special case timeId 0L, then the counter starts at 1 instead of 0
                    if (timeId == 0L) {
                        counter = 1;
                    }
                } else {
                    counter = streamIds.last().getCounter() + 1;
                }
            }
            streamId = new StreamId(timeId, counter);
            validateStreamIdMinimum(streamId);
            streamIds.add(streamId);
            dataValues.put(streamId, values);
        } else {
            throw new IllegalStreamItemIdException(String.format("ERR: bad id format: %s", itemId));
        }
        notifyAll();
        return streamId;
    }

    private void validateStreamIdMinimum(StreamId streamId) throws IllegalStreamItemIdException {
        if (StreamId.compare(streamId, StreamId.MIN_ID) <= 0) {
            throw new IllegalStreamItemIdException(String.format(
                    "ERR The ID specified in XADD must be greater than 0-0"));
        }
        if (streamIds.size() > 0 && StreamId.compare(streamId, streamIds.last()) <= 0) {
            throw new IllegalStreamItemIdException(String.format(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }
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
