package org.baylight.redis;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public class TestConstants {
    public static final Clock FIXED_CLOCK_EPOCH = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

    private TestConstants() { }
}
