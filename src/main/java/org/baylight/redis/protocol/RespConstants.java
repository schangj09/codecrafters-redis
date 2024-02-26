package org.baylight.redis.protocol;

public class RespConstants {

    public static final byte[] NULL = "$-1\r\n".getBytes();
    public static final byte[] OK = "+OK\r\n".getBytes();
    
}
