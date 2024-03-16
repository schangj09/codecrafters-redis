package org.baylight.redis;

import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public String toString() {
        return "StoredData [value=" + new String(value) + ", storedAt=" + storedAt
                + ", ttlMillis=" + ttlMillis + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + Objects.hash(storedAt, ttlMillis);
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof StoredData))
            return false;
        StoredData other = (StoredData) obj;
        return Arrays.equals(value, other.value) && storedAt == other.storedAt
                && Objects.equals(ttlMillis, other.ttlMillis);
    }

}
