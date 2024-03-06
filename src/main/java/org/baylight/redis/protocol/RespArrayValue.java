package org.baylight.redis.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespArrayValue implements RespValue {
    RespValue[] values;

    public RespArrayValue(RespValue[] values) {
        this.values = values;
    }

    public RespArrayValue(BufferedInputLineReader reader, RespValueParser valueParser)
            throws IOException {
        values = new RespValue[reader.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = valueParser.parse(reader);
        }
    }

    public int getSize() {
        return values.length;
    }

    public RespValue[] getValues() {
        return values;
    }

    @Override
    public RespType getType() {
        return RespType.ARRAY;
    }

    @Override
    public byte[] asResponse() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + getSize()).getBytes());
            out.write(RespConstants.CRLF);
            out.write(Integer.toString(values.length).getBytes());
            out.write(RespConstants.CRLF);
            for (RespValue value : values) {
                out.write(value.asResponse());
            }
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    /**
     * The array value does not have a sring representation. It always returns null.
     */
    @Override
    public String getValueAsString() {
        return null;
    }

    @Override
    public String toString() {
        return "RespArrayValue [values=" + Arrays.toString(values) + "]";
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
        RespArrayValue other = (RespArrayValue) obj;
        return Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}
