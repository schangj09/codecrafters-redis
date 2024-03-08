package org.baylight.redis;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.io.ResponseStreamWriter;

public class ClientConnection {
    Socket clientSocket;
    InputStream inputStream;
    OutputStream outputStream;
    BufferedInputLineReader reader;
    ResponseStreamWriter writer;

    public ClientConnection(Socket s) throws IOException {
        clientSocket = s;
        inputStream = s.getInputStream();
        reader = new BufferedInputLineReader(inputStream);
        outputStream = s.getOutputStream();
        writer = new ResponseStreamWriter(outputStream);
    }

    String getConnectionString() {
        return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
    }

    public boolean isClosed() {
        return clientSocket.isClosed();
    }
}
