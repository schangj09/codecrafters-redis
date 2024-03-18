package org.baylight.redis;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.baylight.redis.protocol.RespValue;

public interface TestConstants {
    long CLOCK_MILLIS = 90000;
    Clock FIXED_CLOCK = Clock.fixed(Instant.ofEpochMilli(CLOCK_MILLIS), ZoneOffset.UTC);

    default String encodeResponse(RespValue value) {
        return new String(value.asResponse()).replace("\r\n", "\\r\\n");
    }

    default String encodeResponse(byte[] result) {
        return new String(result).replace("\r\n", "\\r\\n");
    }
}
