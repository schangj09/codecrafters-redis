package org.baylight.redis.protocol;

import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

/**
 * A RESP simple string value.
 **/
public class RespSimpleStringValue extends RespValueBase {
    private final String value;

    public RespSimpleStringValue(String s) {
        super(RespType.SIMPLE_STRING);
        this.value = s;
    }

    public RespSimpleStringValue(BufferedInputLineReader reader) throws IOException {
        this(reader.readLine());
    }

    @Override
    public byte[] asResponse() {
        return ("+" + value + "\r\n").getBytes();
    }

    @Override
    public String getValueAsString() {
        return value;
    }

    @Override
    public String toString() {
        return "RespSimpleStringValue [s=" + value + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass()) {
            return false;
        }
        RespSimpleStringValue other = (RespSimpleStringValue) obj;
        return value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

}
