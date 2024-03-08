package org.baylight.redis.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BufferedResponseStreamWriter {
    private final BufferedOutputStream out;

    public BufferedResponseStreamWriter(OutputStream out) {
        this.out = (out instanceof BufferedOutputStream) ? (BufferedOutputStream) out
                : new BufferedOutputStream(out);
    }

    public void writeFlush(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

}
