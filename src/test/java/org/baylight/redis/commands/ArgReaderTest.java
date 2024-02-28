package org.baylight.redis.commands;

import java.util.Map;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.testng.annotations.Test;

class ArgReaderTest implements WithAssertions {

    @Test
    public void testReadArgsRequired() {
        String[] argSpec = { ":int", "c1:string", "c2" };
        ArgReader reader = new ArgReader("mycmd", argSpec);
        RespValue[] args = {
                new RespSimpleStringValue("23"),
                new RespSimpleStringValue("c1"),
                new RespSimpleStringValue("v1"),
                new RespSimpleStringValue("c2")
        };
        Map<String, RespValue> options = reader.readArgs(args);

        assertThat(options).hasSize(3);
        assertThat(options).isEqualTo(
                Map.of(
                        "0", new RespSimpleStringValue("23"),
                        "c1", new RespSimpleStringValue("v1"),
                        "c2", RespConstants.NULL_VALUE));
    }
}