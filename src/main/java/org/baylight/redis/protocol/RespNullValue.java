package org.baylight.redis.protocol;

public class RespNullValue extends RespBulkString {

    public static RespNullValue INSTANCE = new RespNullValue();

    private RespNullValue() {
        super(new byte[] {});
    }

    @Override
    public byte[] asResponse() {
        return RespConstants.NULL;
    }

    @Override
    public String toString() {
        return "RespNullValue [RespBulkString $-1\\r\\n]";
    }

}
