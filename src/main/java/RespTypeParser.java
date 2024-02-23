import java.io.IOException;

public class RespTypeParser {

    public static RespValue parse(BufferedInputLineReader reader) throws IOException {
        int type = reader.read();

        RespType respType = RespType.of((char)type);
        return switch (respType) {
            case SIMPLE_STRING -> new RespSimpleStringValue(reader);
            //case SIMPLE_ERROR -> new SimpleErrorRespValue(reader);
            case INTEGER -> new RespInteger(reader);
            case BULK_STRING -> new RespBulkString(reader);
            case ARRAY -> new RespArrayValue(reader);
            case null, default -> {
                System.out.println("Unknown type: " + type);
                yield null;
            }
		};
    }
}
