package org.baylight.redis.streams;

import java.util.List;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.junit.jupiter.api.Test;

public class RedisStreamDataTest implements WithAssertions, TestConstants {

    @Test
    void testAdd() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        assertThat(data.add("11-2", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(11, 2));
        assertThat(data.add("11-4", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(11, 4));
        assertThat(data.add("12-0", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(12, 0));
    }

    @Test
    void testAddAutoGenerateSequence() throws Exception {
        RedisStreamData data = new RedisStreamData("test");

        // Note: timestamp 0 starts with counter 1, but any other timestamp starts with counter 0
        assertThat(data.add("0-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(0, 1));
        assertThat(data.add("0-*", FIXED_CLOCK, new RespValue[] {})).isEqualTo(new StreamId(0, 2));

        // start timestamp 11 with hardcoded counter 2
        data.add("11-2", FIXED_CLOCK, new RespValue[] {});
        assertThat(data.add("11-*", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(11, 3));

        // expect timestamp 12 to start with counter 0
        assertThat(data.add("12-*", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(12, 0));
    }

    @Test
    void testAddAutoGenerate() throws Exception {
        RedisStreamData data = new RedisStreamData("test");

        // Note: timestamp 0 starts with counter 1, but any other timestamp starts with counter 0
        assertThat(data.add("*", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(CLOCK_MILLIS, 0));
        assertThat(data.add("*", FIXED_CLOCK, new RespValue[] {}))
                .isEqualTo(new StreamId(CLOCK_MILLIS, 1));

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

    @Test
    void testQueryOpenSequence() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval1") });
        data.add("10-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval2") });
        data.add("10-2", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval3") });
        data.add("11-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval4") });

        assertThat(data.queryRange("10", "10")).isEqualTo(List.of(
                new StreamValue(StreamId.of(10, 1),
                        new RespValue[] { new RespSimpleStringValue("testval2") }),
                new StreamValue(StreamId.of(10, 2),
                        new RespValue[] { new RespSimpleStringValue("testval3") })));
    }

    @Test
    void testQueryFixedSequence() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval1") });
        data.add("10-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval2") });
        data.add("10-2", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval3") });
        data.add("11-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval4") });

        assertThat(data.queryRange("10-2", "11-3")).isEqualTo(List.of(
                new StreamValue(StreamId.of(10, 2),
                        new RespValue[] { new RespSimpleStringValue("testval3") }),
                new StreamValue(StreamId.of(11, 1),
                        new RespValue[] { new RespSimpleStringValue("testval4") })));
    }

    @Test
    void testQueryFromStart() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("0-*", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval0") });
        data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval1") });
        data.add("10-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval2") });
        data.add("10-2", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval3") });
        data.add("11-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval4") });

        assertThat(data.queryRange("-", "10")).isEqualTo(List.of(
                new StreamValue(StreamId.of(0, 1),
                        new RespValue[] { new RespSimpleStringValue("testval0") }),
                new StreamValue(StreamId.of(9, 1),
                        new RespValue[] { new RespSimpleStringValue("testval1") }),
                new StreamValue(StreamId.of(10, 1),
                        new RespValue[] { new RespSimpleStringValue("testval2") }),
                new StreamValue(StreamId.of(10, 2),
                        new RespValue[] { new RespSimpleStringValue("testval3") })));
    }

    @Test
    void testQueryToEnd() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval1") });
        data.add("10-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval2") });
        data.add("10-*", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval3") });
        data.add("11-*", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval4") });

        assertThat(data.queryRange("10", "+")).isEqualTo(List.of(
                new StreamValue(StreamId.of(10, 1),
                        new RespValue[] { new RespSimpleStringValue("testval2") }),
                new StreamValue(StreamId.of(10, 2),
                        new RespValue[] { new RespSimpleStringValue("testval3") }),
                new StreamValue(StreamId.of(11, 0),
                        new RespValue[] { new RespSimpleStringValue("testval4") })));
    }

    @Test
    void testQueryBadIdFormat_throwsException() {
        RedisStreamData data = new RedisStreamData("test");
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("10a", "11-3"))
                .withMessage("ERR: bad query id format: 10a");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("10a-2", "11-3"))
                .withMessage("ERR: bad query id format: 10a-2");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("10-2a", "11-3"))
                .withMessage("ERR: bad query id sequence format: 10-2a");

        // Note: it is not an exception for end < start
        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("11-3", "10a"))
                .withMessage("ERR: bad query id format: 10a");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("11-3", "10a-2"))
                .withMessage("ERR: bad query id format: 10a-2");

        assertThatExceptionOfType(IllegalStreamItemIdException.class)
                .isThrownBy(() -> data.queryRange("11-3", "10-2a"))
                .withMessage("ERR: bad query id sequence format: 10-2a");
    }

    @Test
    void testReadNextValuesFromFixed() throws Exception {
        RedisStreamData data = new RedisStreamData("test");
        data.add("0-*", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval0") });
        data.add("9-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval1") });
        assertThat(data.readNextValues(5, StreamId.MIN_ID, null)).isEqualTo(
                List.of(
                        new StreamValue(
                                StreamId.of(0, 1),
                                new RespValue[] { new RespSimpleStringValue("testval0") }),
                        new StreamValue(
                                StreamId.of(9, 1),
                                new RespValue[] { new RespSimpleStringValue("testval1") })));

        data.add("10-0", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval2") });
        data.add("10-1", FIXED_CLOCK,
                new RespValue[] { new RespSimpleStringValue("testval3") });

        assertThat(data.readNextValues(5, StreamId.of(9, 1), null)).isEqualTo(
                List.of(
                        new StreamValue(
                                StreamId.of(10, 0),
                                new RespValue[] { new RespSimpleStringValue("testval2") }),
                        new StreamValue(
                                StreamId.of(10, 1),
                                new RespValue[] { new RespSimpleStringValue("testval3") })));
        assertThat(data.readNextValues(5, StreamId.of(10, 1), null)).isEmpty();
    }

}
