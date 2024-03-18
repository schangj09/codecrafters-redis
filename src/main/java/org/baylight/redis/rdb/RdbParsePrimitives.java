package org.baylight.redis.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RdbParsePrimitives {

    FileReader file;

    public RdbParsePrimitives(BufferedInputStream file) {
        this.file = new FileReader(file);
    }

    public char[] readChars(int length) throws IOException {
        char[] c = new char[length];
        for (int i = 0; i < length; i++) {
            c[i] = (char) file.read();
        }
        return c;
    }

    public boolean readHeader() throws IOException {
        return ByteUtils.compareToString(readChars(5), ("REDIS"));
    }

    public OpCode readCode() throws IOException {
        return OpCode.fromCode(file.read());
    }

    public EncodedValue readValue(int next) throws IOException {
        return EncodedValue.parseValue(next, file);
    }

    public int readInt() throws IOException {
        return ByteUtils.decodeInt(file.readNBytes(4));
    }

    public int readIntLittleEnd() throws IOException {
        return ByteUtils.decodeIntLittleEnd(file.readNBytes(4));
    }

    public long readLongLittleEnd() throws IOException {
        return ByteUtils.decodeLongLittleEnd(file.readNBytes(8));
    }

    public byte[] readNBytes(int length) throws IOException {
        byte[] val = file.readNBytes(length);
        if (val.length != length) {
            throw new IOException("Unexpected end of file");
        }
        return val;
    }

    public int read() throws IOException {
        return file.read();
    }

    static class FileReader extends InputStream {
        private BufferedInputStream file;
        int readCount = 0;

        FileReader(BufferedInputStream file) {
            this.file = file;
        }

        @Override
        public int read() throws IOException {
            if (file.available() <= 0) {
                throw new IOException("Unexpected end of file at index: " + readCount);
            }
            readCount++;
            return file.read();
        }
        
    }
}
