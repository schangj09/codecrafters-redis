package org.baylight.redis.protocol;

import java.util.List;

/**
 * Inteface for RESP protocol values, such as simple string, bulk string, integer values, etc.
 */
public interface RespValue {
    /**
     * Specifies the RESP type of the value.
     * @return the type
     */
    RespType getType();

    /**
     * Each value should override to provide a human readable string representation of the RESP value.
     * This may include indicate type information and other metadata.
     * @return the string representation
     */
    @Override
    String toString();

    /**
     * Each type should provide type specific equals implementation.
     * @param obj the object to compare
     * @return true if they are equal
     */
    @Override
    boolean equals(Object obj);

    /**
     * Each type should provide type specific hash code implementation.
     * @return the hash code
     */ 
    @Override
    int hashCode();

    /**
     * The byte array representation of the RESP type.
     * @return the byte array
     */
    default byte[] asResponse() {
        return new byte[] {};
    }

    /**
     * The string representation of the RESP value. This should not include metadata, but rather 
     * return the value itself encoded as a String. It may return null if the value can not be 
     * converted to a string.
     * @return the value as a string or null
     */
    String getValueAsString();

    /**
     * The integral representation of the RESP value. This will return null if the value can not be 
     * converted to a 64-bit long integer.
     * @return the value as a long or null
     */
    default Long getValueAsLong() {
        try {   
            return Long.parseLong(getValueAsString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Helper to check for simple string RESP type.
     * @return true if the value is a simple string
     */
    default boolean isSimpleString() {
        return getType() == RespType.SIMPLE_STRING;
    }

    /**
     * Helper to check for bulk string RESP type.
     * @return true if the value is a bulk string
     */
    default boolean isBulkString() {
        return getType() == RespType.BULK_STRING;
    }

    /**
     * Helper to check for integer RESP type.
     * @return true if the value represents a 64-bit integer
     */
    default boolean isInteger() {
        return getType() == RespType.INTEGER || getValueAsLong() != null;
    }

    /**
     * Helper to cast or convert the value to RespBulkString.
     * @return the value as a bulk string or null if the value can not be converted to a bulk string
     */
    default RespBulkString asBulkString() {
        return isBulkString()
                ? (RespBulkString) this
                : getValueAsString() != null
                    ? new RespBulkString(getValueAsString().getBytes())
                    : null;
    }

    public static RespArrayValue array(List<RespValue> values) {
        return new RespArrayValue(values.toArray(new RespValue[] {}));
    }

    public static RespArrayValue array(RespValue... values) {
        return new RespArrayValue(values);
    }

    public static RespSimpleStringValue simpleString(String value) {
        return new RespSimpleStringValue(value);
    }

}
