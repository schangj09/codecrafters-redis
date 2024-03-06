package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespValueParser {

    public RespValue parse(BufferedInputLineReader reader) throws IOException {
        int type = reader.read();

        RespType respType = RespType.of((char)type);
        return switch (respType) {
            case SIMPLE_STRING -> new RespSimpleStringValue(reader);
            //case SIMPLE_ERROR -> new SimpleErrorRespValue(reader);
            case INTEGER -> new RespInteger(reader);
            case BULK_STRING -> new RespBulkString(reader);
            case ARRAY -> new RespArrayValue(reader, this);
            case null, default -> {
                System.out.println("Unknown type: " + type);
                yield null;
            }
		};
    }
}
