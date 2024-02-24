import java.io.IOException;

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
    public String toString() {
        return "RespInteger [value=" + value + "]";
    }

}
