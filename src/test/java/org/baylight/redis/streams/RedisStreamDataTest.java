package org.baylight.redis.streams;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.junit.jupiter.api.Test;

public class RedisStreamDataTest implements WithAssertions, TestConstants {

    @Test
    void testAdd() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        assertThat(data.add("11-2", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(11, 2));
        assertThat(data.add("11-4", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(11, 4));
        assertThat(data.add("12-0", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(12, 0));
    }

    @Test
    void testAddAutoGenerateSequence() throws Exception {
        RedisStreamData data = new RedisStreamData("test");

        // Note: timestamp 0 starts with counter 1, but any other timestamp starts with counter 0
        assertThat(data.add("0-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(0, 1));
        assertThat(data.add("0-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(0, 2));

        // start timestamp 11 with hardcoded counter 2
        data.add("11-2", FIXED_CLOCK, new RespValue[] {});
        assertThat(data.add("11-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(11, 3));

        // expect timestamp 12 to start with counter 0
        assertThat(data.add("12-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(12, 0));
    }

    @Test
    void testAddAutoGenerate() throws Exception {
        RedisStreamData data = new RedisStreamData("test");

        // Note: timestamp 0 starts with counter 1, but any other timestamp starts with counter 0
        assertThat(data.add("*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(CLOCK_MILLIS, 0));
        assertThat(data.add("*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(CLOCK_MILLIS, 1));

    }

    @Test
    void testAddBadIdFormat_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("abc-1", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR: bad id format: abc-1");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("*-*", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR: bad id format: *-*");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("1-**", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR: bad id format: 1-**");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("abc", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR: bad id format: abc");
    }

    @Test
    void testAdd00_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        // the minimum itemId for the stream is 0-0
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("0-0", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR The ID specified in XADD must be greater than 0-0");
    }

    @Test
    void testAddIdLessThanEqualLast_throwsException() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("9-3", FIXED_CLOCK, new RespValue[] { new RespSimpleStringValue("testval") });

        // the minimum itemId for the stream is 0-0
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("0-0", FIXED_CLOCK, new RespValue[] {}))
                .withMessage("ERR The ID specified in XADD must be greater than 0-0");

        // equals
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("9-3", FIXED_CLOCK, new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        // counter is less than
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("9-1", FIXED_CLOCK, new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        // time is less than
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.add("8-0", FIXED_CLOCK, new RespValue[] {}))
                .withMessage(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    @Test
    void testGetData() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        StreamId streamId = data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval") });
        assertThat(data.getData(streamId))
                .isEqualTo(new RespValue[] { new RespSimpleStringValue("testval") });
    }

}
