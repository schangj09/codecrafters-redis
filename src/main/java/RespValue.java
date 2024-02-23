public interface RespValue {
    RespType getType();

    String toString();

    default boolean isBulkString() {
        return getType() == RespType.BULK_STRING;
    }

    default boolean isInteger() {
        return getType() == RespType.INTEGER;
    }
}
