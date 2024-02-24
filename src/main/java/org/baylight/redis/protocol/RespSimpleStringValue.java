import java.io.IOException;

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

}
