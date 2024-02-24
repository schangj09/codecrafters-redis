package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class EchoCommand extends RedisCommand {

    RespBulkString bulkStringArg;

    public EchoCommand() {
        super(Type.ECHO);
    }

    public EchoCommand(RespBulkString bulkStringArg) {
        super(Type.ECHO);
        this.bulkStringArg = bulkStringArg;
    }

    @Override
    public void setArgs(RespValue[] values) {
        if (values.length != 2) {
            throw new RuntimeException(String.format("EchoCommand: Invalid number of arguments: %d", values.length));
        }
        if (!values[1].isBulkString()) {
            throw new RuntimeException(String.format("EchoCommand: Invalid argument: %s", values[1]));
        }
        this.bulkStringArg = (RespBulkString) values[1];
    }

    @Override
    public byte[] getResponse() {
        return bulkStringArg != null ? bulkStringArg.asResponse() : new byte[]{};
    }

    public static EchoCommand parse(BufferedInputLineReader reader) throws IOException {
        String line = reader.readLine();

        // parse line as an Integer
        int size = Integer.parseInt(line);
        // read the bulk string
        byte[] arg = new byte[size];
        int n = reader.read(arg, 0, size);
        if (n != size) {
            throw new RuntimeException(String.format("Error reading bulk string: expected %d bytes, got %d bytes"));
        }
        return new EchoCommand(new RespBulkString(arg));
    }

    @Override
    public String toString() {
        return "EchoCommand [bulkStringArg=" + bulkStringArg + "]";
    }
}
