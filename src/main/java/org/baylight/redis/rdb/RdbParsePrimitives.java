package org.baylight.redis.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;

public class RdbParsePrimitives {

    private BufferedInputStream file;

    RdbParsePrimitives(BufferedInputStream file) {
        this.file = file;
    }

    char[] readChars(int length) throws IOException {
        char[] c = new char[length];
        for (int i = 0; i < length; i++) {
            c[i] = (char) file.read();
        }
        return c;
    }

    boolean readHeader() throws IOException {
        return ByteUtils.compareToString(readChars(5), ("REDIS"));
    }

    OpCode readCode() throws IOException {
        return OpCode.fromCode(file.read());
    }

    EncodedValue readValue(int next) throws IOException {
        return EncodedValue.parseValue(next, file);
    }

    byte[] readNBytes(int length) throws IOException {
        byte[] val = file.readNBytes(length);
        if (val.length != length) {
            throw new IOException("Unexpected end of file");
        }
        return val;
    }

}
