package org.baylight.redis;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.baylight.redis.io.BufferedInputLineReader;

public class ClientConnection {
    Socket clientSocket;
    InputStream inputStream;
    OutputStream outputStream;
    BufferedInputLineReader reader;
    BufferedOutputStream writer;

    public ClientConnection(Socket s) throws IOException {
        clientSocket = s;
        inputStream = s.getInputStream();
        reader = new BufferedInputLineReader(new BufferedInputStream(inputStream));
        outputStream = s.getOutputStream();
        writer = new BufferedOutputStream(outputStream);
    }

    String getConnectionString() {
        return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
    }

    public void writeFlush(byte[] bytes) throws IOException {
        writer.write(bytes);
        writer.flush();
    }

    public boolean isClosed() {
        return clientSocket.isClosed();
    }
}
