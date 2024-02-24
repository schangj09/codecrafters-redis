package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespSimpleStringValue implements RespValue {

    private String s;

    public RespSimpleStringValue(String s) {
        this.s = s;
    }

    public RespSimpleStringValue(BufferedInputLineReader reader) throws IOException {
        this(reader.readLine());
    }

    @Override
    public RespType getType() {
        return RespType.SIMPLE_STRING;
    }

    @Override
    public byte[] asResponse() {
        return ("+" + s + "\r\n").getBytes();
    }

    @Override
    public String getValueAsString() {
        return s;
    }

    @Override
    public String toString() {
        return "RespSimpleStringValue [s=" + s + "]";
    }

}
