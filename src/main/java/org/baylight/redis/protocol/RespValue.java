package org.baylight.redis.protocol;

public interface RespValue {
    RespType getType();

    String toString();

    default byte[] asResponse() {
        return new byte[] {};
    }

    default String getValueAsString() {
        return toString();
    }

    default Long getValueAsLong() {
        return null;
    }

    default boolean isSimpleString() {
        return getType() == RespType.SIMPLE_STRING;
    }

    default boolean isBulkString() {
        return getType() == RespType.BULK_STRING;
    }

    default boolean isInteger() {
        return getType() == RespType.INTEGER;
    }

    default RespBulkString asBulkString() {
        return isBulkString()
                ? (RespBulkString) this
                : new RespBulkString(getValueAsString().getBytes());
    }
}
