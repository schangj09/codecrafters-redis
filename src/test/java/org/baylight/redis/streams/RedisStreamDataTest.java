package org.baylight.redis.streams;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.junit.jupiter.api.Test;

public class RedisStreamDataTest implements WithAssertions {
    @Test
    void testAdd() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        assertThat(data.add("11-2", new RespValue[] {})).isEqualTo(new StreamId(11, 2));
        assertThat(data.add("11-4", new RespValue[] {})).isEqualTo(new StreamId(11, 4));
        assertThat(data.add("12-0", new RespValue[] {})).isEqualTo(new StreamId(12, 0));
    }

    @Test
    void testAddBadId_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("abc", new RespValue[] {}))
                .withMessage("ERR: unknown id abc");
    }

    @Test
    void testAdd00_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        // the minimum itemId for the stream is 0-0
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("0-0", new RespValue[] {}))
                .withMessage("ERR The ID specified in XADD must be greater than 0-0");
    }

    @Test
    void testAddIdLessThanEqualLast_throwsException() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("9-3", new RespValue[] { new RespSimpleStringValue("testval") });

        // the minimum itemId for the stream is 0-0
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("0-0", new RespValue[] {}))
                .withMessage("ERR The ID specified in XADD must be greater than 0-0");

        // equals
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("9-3", new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        // counter is less than
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("9-1", new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        // time is less than
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("8-0", new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    @Test
    void testGetData() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        StreamId streamId = data.add("9-1",
                new RespValue[] { new RespSimpleStringValue("testval") });
        assertThat(data.getData(streamId))
                .isEqualTo(new RespValue[] { new RespSimpleStringValue("testval") });
    }

}
