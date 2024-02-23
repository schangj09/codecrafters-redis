public class PingCommand extends RedisCommand {

    public PingCommand() {
        super(Type.PING);
    }

    public static String NAME = "ping";

    @Override
    public byte[] getResponse() {
]        return "+PONG\r\n".getBytes();
    }

    @Override
    public String toString() {
        return "PingCommand";
    }
}
