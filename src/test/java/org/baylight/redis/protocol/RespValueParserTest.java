package org.baylight.redis.protocol;

import java.io.ByteArrayInputStream;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.io.BufferedInputLineReader;
import org.testng.annotations.Test;

public class RespValueParserTest implements WithAssertions {
    @Test
    void testParseSimpleString() throws Exception {
        String inpuString = "+OK\r\n";
        BufferedInputLineReader reader = new BufferedInputLineReader(new ByteArrayInputStream(inpuString.getBytes()));
        assertThat(new RespValueParser().parse(reader)).isEqualTo(new RespSimpleStringValue("OK"));
    }

    @Test
    void testParseIntValue() throws Exception {
        String inpuString = ":-4500000005\r\n";
        BufferedInputLineReader reader = new BufferedInputLineReader(new ByteArrayInputStream(inpuString.getBytes()));
        assertThat(new RespValueParser().parse(reader)).isEqualTo(new RespInteger(-1 * 4500000005L));
    }

    @Test
    void testParseBulkString() throws Exception {
        String value = "a\r\nb\r\ncd";
        String inpuString = "$" + value.length() + "\r\n" + value + "\r\n";
        BufferedInputLineReader reader = new BufferedInputLineReader(new ByteArrayInputStream(inpuString.getBytes()));
        assertThat(new RespValueParser().parse(reader)).isEqualTo(new RespBulkString(value.getBytes()));
    }

    @Test
    void testParseArray() throws Exception {
        String bulkValue = "a\r\nb\r\ncd";
        String bulkString = "$" + bulkValue.length() + "\r\n" + bulkValue + "\r\n";
        StringBuffer sb = new StringBuffer();
        sb.append("*3\r\n");
        sb.append(bulkString);
        sb.append(":22\r\n");
        sb.append("+yes\r\n");
        String inpuString = sb.toString();
        BufferedInputLineReader reader = new BufferedInputLineReader(new ByteArrayInputStream(inpuString.getBytes()));
        assertThat(new RespValueParser().parse(reader)).isEqualTo(
                new RespArrayValue(new RespValue[] {
                        new RespBulkString(bulkValue.getBytes()),
                        new RespInteger(22),
                        new RespSimpleStringValue("yes")
                }));
    }
}
