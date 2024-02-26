package org.baylight.redis.protocol;
import java.io.IOException;

import org.baylight.redis.io.BufferedInputLineReader;

public class RespBulkString implements RespValue {
    private final byte[] value;

    public RespBulkString(byte[] value) {
        this.value = value;
    }

    public RespBulkString(BufferedInputLineReader reader) throws IOException {
        value = reader.readNBytes(reader.readInt());
        reader.readCRLF();
    }

    public byte[] asResponse() {
        StringBuilder builder = new StringBuilder("$").append(value.length).append("\r\n");
        byte[] prefixBytes = builder.toString().getBytes();
        int n = prefixBytes.length + value.length + 2;

        byte[] result = new byte[n];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(value, 0, result, prefixBytes.length, value.length);
        result[n - 2] = '\r';
        result[n - 1] = '\n';
        return result;
    }

    @Override
    public RespType getType() {
        return RespType.BULK_STRING;
    }

    @Override
    public String toString() {
        return "BulkString [length=" + value.length + ", value=" + truncValueString(15) + "]";
    }

    private String truncValueString(int trunclen) {
        trunclen = Math.min(value.length, trunclen);
         StringBuilder sb = new StringBuilder(new String(value, 0, trunclen));
         if (value.length > trunclen) {
             sb.append("...");
         }
         return sb.toString();
    }

    public byte[] getValue() {
        return value;
    }

    public String getValueAsString() {
        return new String(value);
    }

}
