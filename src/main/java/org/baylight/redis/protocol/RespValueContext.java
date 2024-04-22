package org.baylight.redis.protocol;

import org.baylight.redis.ClientConnection;

public class RespValueContext {
    private ClientConnection clientConnection;
    private final long startBytesOffset;
    private final int numBytesRead;

    public RespValueContext(ClientConnection clientConnection, long startBytesOffset,
            int numBytesRead) {
        this.clientConnection = clientConnection;
        this.startBytesOffset = startBytesOffset;
        this.numBytesRead = numBytesRead;
    }

    /**
     * @return the clientConnection
     */
    public ClientConnection getClientConnection() {
        return clientConnection;
    }

    /**
     * @return the startBytesOffset
     */
    public long getStartBytesOffset() {
        return startBytesOffset;
    }

    /**
     * @return the numBytesRead
     */
    public int getNumBytesRead() {
        return numBytesRead;
    }

}
