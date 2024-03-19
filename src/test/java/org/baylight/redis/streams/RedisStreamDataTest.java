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
    }

    @Test
    void testAddBadId_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("abc", new RespValue[] {}))
                .withMessage("ERR: unknown id abc");
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
