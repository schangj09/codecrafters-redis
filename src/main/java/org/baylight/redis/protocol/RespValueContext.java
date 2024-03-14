package org.baylight.redis.protocol;

public class RespValueContext {
    private final long startBytesOffset;
    private final int numBytesRead;

    public RespValueContext(long startBytesOffset, int numBytesRead) {
        this.startBytesOffset = startBytesOffset;
        this.numBytesRead = numBytesRead;
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
