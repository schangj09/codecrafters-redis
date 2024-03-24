package org.baylight.redis.streams;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.baylight.redis.protocol.RespValue;

public class RedisStreamData {
    public static final int MAX_READ_COUNT = 100;
    private final String streamKey;
    private final OrderedArrayList<StreamId> streamIds = new OrderedArrayList<>();
    private final Map<StreamId, RespValue[]> dataValues = new HashMap<>();

    public RedisStreamData(String streamKey) {
        this.streamKey = streamKey;
    }

    public StreamId add(String itemId, Clock clock, RespValue[] values)
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
        } else if ("*".equals(itemId)) {
            // autogenerate the time and counter
            long now = clock.millis();
            int counter = 0;
            if (streamIds.size() > 0 && streamIds.last().getTimeId() == now) {
                counter = streamIds.last().getCounter() + 1;
            }
            streamId = new StreamId(now, counter);
        } else {
            throw new IllegalStreamItemIdException(String.format("ERR: bad id format: %s", itemId));
        }
        validateStreamIdMinimum(streamId);
        streamIds.add(streamId);
        dataValues.put(streamId, values);
        // notify waiters if any
        StreamsWaitManager.INSTANCE.addNotify(streamKey);
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

    public List<StreamValue> readNextValues(int count, StreamId startId) {
        count = Math.min(count, MAX_READ_COUNT);
        if (count == 0) {
            return List.of();
        }
        int index = streamIds.findNext(startId);
        if (index == streamIds.size()) {
            return List.of();
        }
        List<StreamValue> values = new ArrayList<>();
        int end = index + count;
        for (int i = index; i < end && i < streamIds.size(); i++) {
            StreamId nextId = streamIds.get(i);
            StreamValue value = new StreamValue(nextId, dataValues.get(nextId));
            values.add(value);
        }
        return values;
    }

    public List<StreamValue> queryRange(String start, String end)
            throws IllegalStreamItemIdException {
        StreamId startId = parseForQuery(start, true);
        StreamId endId = parseForQuery(end, false);
        List<StreamId> ids = streamIds.range(startId, endId);
        return ids.stream().map(id -> new StreamValue(id, dataValues.get(id))).toList();
    }

    private StreamId parseForQuery(String param, boolean isStart)
            throws IllegalStreamItemIdException {
        if (param.equals("-")) {
            return StreamId.MIN_ID;
        }
        if (param.equals("+")) {
            return StreamId.MAX_ID;
        }
        String[] ids = param.split("-");
        long timeId = 0;
        if (ids[0].length() > 0) {
            try {
                timeId = Long.parseLong(ids[0]);
            } catch (NumberFormatException e) {
                throw new IllegalStreamItemIdException(
                        String.format("ERR: bad query id format: %s", param));
            }
        }

        int counter = 0;
        if (ids.length == 2) {
            try {
                counter = Integer.parseInt(ids[1]);
            } catch (NumberFormatException e) {
                if (!"*".equals(ids[1])) {
                    throw new IllegalStreamItemIdException(
                            String.format("ERR: bad query id sequence format: %s", param));
                }
            }
            return StreamId.of(timeId, counter);
        } else {
            counter = isStart ? 0 : Integer.MAX_VALUE;
        }
        return StreamId.of(timeId, counter);
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
