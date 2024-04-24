package org.baylight.redis;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.commands.RedisCommandConstructor;
import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueContext;
import org.baylight.redis.protocol.RespValueParser;

public interface TestConstants {
    RedisCommandConstructor REDIS_COMMAND_CONSTRUCTOR = new RedisCommandConstructor();
    RespValueParser RESP_VALUE_PARSER = new RespValueParser();
    long CLOCK_MILLIS = 90000;
    Clock FIXED_CLOCK = Clock.fixed(Instant.ofEpochMilli(CLOCK_MILLIS), ZoneOffset.UTC);

    default String encodeResponse(String value) {
        return value.replace("\r\n", "\\r\\n");
    }

    default String encodeResponse(RespValue value) {
        return new String(value.asResponse()).replace("\r\n", "\\r\\n");
    }

    default String encodeResponse(byte[] result) {
        return new String(result).replace("\r\n", "\\r\\n");
    }

    public static RespValue valueOf(String value) {
        try {
            return RESP_VALUE_PARSER.parse(
                    new BufferedInputLineReader(new ByteArrayInputStream(value.getBytes())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static RedisCommand commandOf(String value) {
        RespValue respValue = valueOf(value);
        if (respValue instanceof RespArrayValue arrayValue) {
            arrayValue.setContext(new RespValueContext(null, 100, 101));
            return REDIS_COMMAND_CONSTRUCTOR.newCommandFromValue(arrayValue);
        }
        throw new RuntimeException("Command value must be an array: " + value);
    }

}
