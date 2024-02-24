package org.baylight.redis.protocol;
import java.io.IOException;
import java.util.Arrays;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespArrayValue implements RespValue {
    RespValue[] values;

    public RespArrayValue(BufferedInputLineReader reader) throws IOException {
        values = new RespValue[reader.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = RespTypeParser.parse(reader);
        }
    }

    int getSize() {
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
    public String toString() {
        return "RespArrayValue [values=" + Arrays.toString(values) + "]";
    }
}
