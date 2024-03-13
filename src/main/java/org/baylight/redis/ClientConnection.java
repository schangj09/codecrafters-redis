package org.baylight.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.io.BufferedResponseStreamWriter;

public class ClientConnection {
    private Socket clientSocket;
    private InputStream inputStream;
    private OutputStream outputStream;
    private BufferedInputLineReader reader;
    private BufferedResponseStreamWriter writer;
    volatile boolean followerHandshakeComplete = false;

    public ClientConnection(Socket s) throws IOException {
        clientSocket = s;
        inputStream = s.getInputStream();
        reader = new BufferedInputLineReader(new BufferedInputStream(inputStream));
        outputStream = s.getOutputStream();
        writer = new BufferedResponseStreamWriter(new BufferedOutputStream(outputStream));
    }

    String getConnectionString() {
        return clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort();
    }

    public boolean isClosed() {
        return clientSocket.isClosed();
    }

    public void close() throws IOException {
        clientSocket.close();
    }

    public int available() throws IOException {
        return reader.available();
    }

    public long getNumBytesReceived() {
        return reader.getNumBytesReceived();
    }

    /**
     * Temporary package level method for accessing the reader until we refacter the value parser
     * reader into this class.
     * 
     * @return the reader
     */
    BufferedInputLineReader getReader() {
        return reader;
    }

    public void writeFlush(byte[] bytes) throws IOException {
        writer.writeFlush(bytes);
    }

    public void setFollowerHandshakeComplete() {
        followerHandshakeComplete = true;
    }

    public boolean isFollowerHandshakeComplete() {
        return followerHandshakeComplete;
    }

    @Override
    public String toString() {
        return clientSocket.toString();
    }

}
