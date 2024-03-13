package org.baylight.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.io.BufferedResponseStreamWriter;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;

public class ClientConnection {
    private final Socket clientSocket;
    private final RespValueParser valueParser;
    private InputStream inputStream;
    private OutputStream outputStream;
    private BufferedInputLineReader reader;
    private BufferedResponseStreamWriter writer;
    volatile boolean followerHandshakeComplete = false;

    public ClientConnection(Socket clientSocket, RespValueParser valueParser) throws IOException {
        this.clientSocket = clientSocket;
        this.valueParser = valueParser;
        inputStream = clientSocket.getInputStream();
        reader = new BufferedInputLineReader(new BufferedInputStream(inputStream));
        outputStream = clientSocket.getOutputStream();
        writer = new BufferedResponseStreamWriter(new BufferedOutputStream(outputStream));
    }

    RespValue readValue() throws IOException {
        return valueParser.parse(reader);
    }

    byte[] readRDB() throws IOException {
        int val = reader.read();
        if (val != '$') {
            throw new IllegalArgumentException("Expected RDB from leader, got char " + val);
        }
        long len = reader.readLong();
        // TODO: refactor the RDB processing with a stream instead of byte array
        byte[] rdb = new byte[(int) len];
        for (int i = 0; i < len; i++) {
            rdb[i] = (byte) reader.read();
        }
        return rdb;
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
