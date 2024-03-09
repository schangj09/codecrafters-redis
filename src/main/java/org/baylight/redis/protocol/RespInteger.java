package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespInteger implements RespValue {
    private final long value;

    public RespInteger(BufferedInputLineReader reader) throws NumberFormatException, IOException {
        this(reader.readLong());
    }

    public RespInteger(long value) {
        this.value = value;
    }

    @Override
    public RespType getType() {
        return RespType.INTEGER;
    }

    @Override
    public byte[] asResponse() {
        return String.format(":%d\r\n", value).getBytes();
    }

    @Override
    public Long getValueAsLong() {
        return value;
    }

    @Override
    public String getValueAsString() {
        return Long.toString(value);
    }

    @Override
    public String toString() {
        return "RespInteger [value=" + value + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RespInteger other = (RespInteger) obj;
        return value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

}
