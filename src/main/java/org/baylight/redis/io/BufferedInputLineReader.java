package org.baylight.redis.io;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;

public class BufferedInputLineReader extends BufferedInputStream {

    public BufferedInputLineReader(InputStream in) {
        super(in);
    }

    public String readLine() throws IOException {
        // read one byte at a time until a newline character is found
        StringBuilder sb = new StringBuilder();
        int c;

        while ((c = read()) != -1) {
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
        readLine();
    }

}