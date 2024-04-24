package org.baylight.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;

import org.baylight.redis.io.BufferedInputLineReader;
import org.baylight.redis.io.BufferedResponseStreamWriter;
import org.baylight.redis.protocol.RespSimpleErrorValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueBase;
import org.baylight.redis.protocol.RespValueContext;
import org.baylight.redis.protocol.RespValueParser;

public class ClientConnection {
    private final Socket clientSocket;
    private final RespValueParser valueParser;
    private InputStream inputStream;
    private OutputStream outputStream;
    private BufferedInputLineReader reader;
    private BufferedResponseStreamWriter writer;

    public ClientConnection(Socket clientSocket, RespValueParser valueParser) throws IOException {
        this.clientSocket = clientSocket;
        this.valueParser = valueParser;
        inputStream = clientSocket.getInputStream();
        reader = new BufferedInputLineReader(new BufferedInputStream(inputStream));
        outputStream = clientSocket.getOutputStream();
        writer = new BufferedResponseStreamWriter(new BufferedOutputStream(outputStream));
    }

    public RespValue readValue() throws IOException {
        long startBytesOffset = reader.getNumBytesReceived();

        RespValue value = valueParser.parse(reader);

        // set the context for the top-level value from the stream - used for creating a REPLCONF
        // command
        long length = reader.getNumBytesReceived() - startBytesOffset;
        RespValueContext context = new RespValueContext(this, startBytesOffset, (int) length);
        ((RespValueBase) value).setContext(context);
        return value;
    }

    public byte[] readRDB() throws IOException {
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

    @Override
    public String toString() {
        return clientSocket.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSocket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof ClientConnection))
            return false;
        ClientConnection other = (ClientConnection) obj;
        return Objects.equals(clientSocket, other.clientSocket);
    }

    public synchronized void notifyNewValueAvailable() {
        notifyAll();
    }

    public synchronized void waitForNewValueAvailable(long timeoutMillis)
            throws InterruptedException {
        wait(timeoutMillis);
    }

    public void sendError(String message) {
        try {
            writeFlush(new RespSimpleErrorValue(message).asResponse());
        } catch (IOException e) {
            System.out.println(String.format(
                    "ClientConnection: exception while sending error response: %s, %s", message,
                    e.getMessage()));
            e.printStackTrace();
        }
    }

}
