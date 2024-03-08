package org.baylight.redis.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BufferedInputLineReader {
    private final BufferedInputStream in;
    long numBytesRead = 0;

    public BufferedInputLineReader(InputStream in) {
        this.in = (in instanceof BufferedInputStream) ? (BufferedInputStream) in
                : new BufferedInputStream(in);
    }

    public long getNumBytesReceived() {
        return numBytesRead;
    }

    public int available() throws IOException {
        return in.available();
    }

    public int read() throws IOException {
        int c = in.read();
        if (c != -1) {
            numBytesRead++;
        }
        return c;
    }

    public byte[] readNBytes(int n) throws IOException {
        byte[] b = in.readNBytes(n);
        numBytesRead += b.length;
        return b;
    }

    public String readLine() throws IOException {
        // read one byte at a time until a newline character is found
        StringBuilder sb = new StringBuilder();
        int c;

        while ((c = read()) != -1) {
            numBytesRead++;
            if (c == '\n') {
                break;
            }
            if (c != '\r') { // ignore carriage returns
                sb.append((char) c);
            }
        }
        return sb.toString();
    }

    public int readInt() throws NumberFormatException, IOException {
        return Integer.parseInt(readLine());
    }

    public long readLong() throws NumberFormatException, IOException {
        return Long.parseLong(readLine());
    }

    public void readCRLF() throws IOException {
        if (read() != '\r' || read() != '\n') {
            throw new IOException("Expected CRLF");
        }
    }

    public boolean readOptionalCRLF() throws IOException {
        if (in.available() == 0) {
            return false;
        } else {
            readCRLF();
            return true;
        }
    }

}