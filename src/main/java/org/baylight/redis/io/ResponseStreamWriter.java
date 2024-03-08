package org.baylight.redis.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ResponseStreamWriter extends BufferedOutputStream {

    public ResponseStreamWriter(OutputStream out) {
        super(out);
    }

    public void writeFlush(byte[] bytes) throws IOException {
        write(bytes);
        flush();
    }

}
