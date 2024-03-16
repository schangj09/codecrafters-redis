package org.baylight.redis;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class StreamBuilder extends ByteArrayOutputStream {

    public void write(String s) throws IOException {
        write(s.getBytes());
    }

    public BufferedInputStream build() {
        // return byte stream from the string
        return new BufferedInputStream(new ByteArrayInputStream(toByteArray()));
    }

}