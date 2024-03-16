package org.baylight.redis.rdb;

public enum OpCode {
    EOF(0xFF), // End of the RDB file
    SELECTDB(0xFE), // Database Selector
    EXPIRETIME(0xFD), // Expire time in seconds, see Key Expiry Timestamp
    EXPIRETIMEMS(0xFC), // Expire time in milliseconds, see Key Expiry Timestamp
    RESIZEDB(0xFB), // Hash table sizes for the main keyspace and expires, see Resizedb information
    AUX(0xFA); // Auxiliary fields. Arbitrary key-value settings, see Auxiliary fields

    private byte code;

    OpCode(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static OpCode fromCode(int code) {
        for (OpCode opCode : OpCode.values()) {
            if (opCode.getCode() == (byte)code) {
                return opCode;
            }
        }
        return null;
    }
}