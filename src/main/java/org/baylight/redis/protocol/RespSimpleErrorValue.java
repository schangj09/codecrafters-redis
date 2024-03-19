package org.baylight.redis.protocol;

import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespSimpleErrorValue extends RespValueBase {
    
    private final String value;

    public RespSimpleErrorValue(String s) {
        super(RespType.SIMPLE_ERROR);
        this.value = s;
    }

    public RespSimpleErrorValue(BufferedInputLineReader reader) throws IOException {
        this(reader.readLine());
    }

    @Override
    public byte[] asResponse() {
        return (RespType.SIMPLE_ERROR.typePrefix + value + "\r\n").getBytes();
    }

    @Override
    public String getValueAsString() {
        return value;
    }

    @Override
    public String toString() {
        return "RespSimpleErrorValue [s=" + value + "]";
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
        RespSimpleErrorValue other = (RespSimpleErrorValue) obj;
        return value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

}
