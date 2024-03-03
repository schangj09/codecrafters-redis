package org.baylight.redis.protocol;

/**
 * Class RespNullValue represents the RESP bulk string null value.
 * This is a singleton and not instantiated directly.
 **/
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
