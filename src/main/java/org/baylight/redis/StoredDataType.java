package org.baylight.redis;

import org.baylight.redis.protocol.RespSimpleStringValue;

public enum StoredDataType {
    STRING, STREAM;

    public RespSimpleStringValue getTypeResponse() {
        return new RespSimpleStringValue(this.name().toLowerCase());
    }

}
