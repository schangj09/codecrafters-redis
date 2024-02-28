package org.baylight.redis.commands;

import java.util.Map;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.testng.annotations.Test;

class ArgReaderTest implements WithAssertions {
    private static final String[] ARG_SPEC = new String[] {
            ":string", // command name
            ":string", // key
            ":string", // value
            "[nx xx]",
            "[get]",
            "[ex:int px:int exat:int pxatt:int keepttl]"
    };

    @Test
    public void testInvalidArgSpec() {
        assertThatThrownBy(() -> new ArgReader("mycmd", new String[] { "myarg:float" }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Invalid type in arg spec: float");
    }

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

    @Test
    public void testReadArgsMissingRequired() {
        String[] argSpec = { ":int", "c1:string", "c2" };
        ArgReader reader = new ArgReader("mycmd", argSpec);
        RespValue[] args = {
                new RespSimpleStringValue("c1"),
                new RespSimpleStringValue("v1"),
                new RespSimpleStringValue("c2")
        };

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage(
                        "mycmd: Invalid arg type, expected integer at index 0: RespSimpleStringValue [s=c1]");
    }

    @Test
    public void testReadArgsRequiredWrongName() {
        String[] argSpec = { ":int", "c1:string", "c2" };
        ArgReader reader = new ArgReader("mycmd", argSpec);
        RespValue[] args = {
                new RespSimpleStringValue("23"),
                new RespSimpleStringValue("c111"),
                new RespSimpleStringValue("v1"),
                new RespSimpleStringValue("c2")
        };

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage(
                        "mycmd: Invalid arg, expected 'c1' at index 1: RespSimpleStringValue [s=c111]");
    }

    @Test
    public void testReadArgsRequiredWrongType() {
        String[] argSpec = { "c1:string", "c2" };
        ArgReader reader = new ArgReader("mycmd", argSpec);
        RespValue[] args = {
                new RespSimpleStringValue("c1"),
                new RespInteger(123),
                new RespSimpleStringValue("c2")
        };

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage(
                        "mycmd: Invalid arg type, expected string at index 1: RespInteger [value=123]");
    }

    @Test
    public void testReadSetCommand() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("nx"),
                new RespSimpleStringValue("px"),
                new RespInteger(345)
        };
        Map<String, RespValue> options = reader.readArgs(args);

        assertThat(options).hasSize(5);
        assertThat(options).isEqualTo(
                Map.of(
                        "0", new RespSimpleStringValue("set"),
                        "1", new RespSimpleStringValue("k1"),
                        "2", new RespBulkString("mybulk\r\nvalue".getBytes()),
                        "nx", RespConstants.NULL_VALUE,
                        "px", new RespInteger(345)));
    }

    @Test
    public void testReadArgsConflictOption() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("xx"),
                new RespSimpleStringValue("nx"),
                new RespInteger(345)
        };
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage(
                        "SET: Invalid arg at index 4: RespSimpleStringValue [s=nx] conflicts with RespSimpleStringValue [s=xx]");
    }

    @Test
    public void testReadArgsUnrecognizedOption() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("xx"),
                new RespInteger(345)
        };
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage("SET: unrecognized arg at index 4, RespInteger [value=345]");
    }

    @Test
    public void testReadArgsUnrecognizedOptionName() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("xx"),
                new RespBulkString("ynot".getBytes())
        };
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage("SET: unrecognized arg at index 4, BulkString [length=4, value=ynot]");
    }

    @Test
    public void testReadArgsMissingValue() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("exat")
        };
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage("SET: Missing value for arg 'exat' at index 4");
    }

    @Test
    public void testReadArgsInvalidValueType() {
        ArgReader reader = new ArgReader("SET", ARG_SPEC);

        RespValue[] args = {
                new RespSimpleStringValue("set"),
                new RespSimpleStringValue("k1"),
                new RespBulkString("mybulk\r\nvalue".getBytes()),
                new RespSimpleStringValue("exat"),
                new RespSimpleStringValue("px")
        };
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> reader.readArgs(args))
                .withMessage("SET: Invalid arg type, expected integer at index 4: RespSimpleStringValue [s=px]");
    }

}