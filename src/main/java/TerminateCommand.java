
public class TerminateCommand extends RedisCommand {

    public TerminateCommand() {
        super(Type.TERMINATE);
    }

    @Override
    public byte[] getResponse() {
        throw new UnsupportedOperationException("Unimplemented method 'getResponse'");
    }

    @Override
    public String toString() {
        return "TerminateCommand []";
    }

}
