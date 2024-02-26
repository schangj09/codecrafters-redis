package org.baylight.redis;

/**
 * Data stored for a Redis map entry
 **/
public class StoredData {
    byte[] value;
    long storedAt;
    Long ttlMillis;

    public StoredData(byte[] value, long storedAt, Long ttlMillis) {
        this.value = value;
        this.storedAt = storedAt;
        this.ttlMillis = ttlMillis;
    }
    public long getStoredAt() {
        return storedAt;
    }
    public Long getTtlMillis() {
        return ttlMillis;
    }
    public boolean isExpired(long currentTimeMillis) {
        return ttlMillis != null && ttlMillis > 0 && (currentTimeMillis - storedAt) > ttlMillis;
    }
    public byte[] getValue() {
        return value;
    }
}
