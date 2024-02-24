package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespInteger implements RespValue {
    private final int value;

    public RespInteger(BufferedInputLineReader reader) throws NumberFormatException, IOException {
        this(reader.readInt());
    }

    public RespInteger(int value) {
        this.value = value;
    }

    @Override
    public RespType getType() {
        return RespType.INTEGER;
    }

    @Override
    public Integer getValueAsInteger() {
        return value;
    }

    @Override
    public String getValueAsString() {
        return Integer.toString(value);
    }

    @Override
    public String toString() {
        return "RespInteger [value=" + value + "]";
    }

}
