package org.baylight.redis.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;

import org.baylight.redis.StoredData;

public class RdbFileParser {

    private final RdbParsePrimitives reader;
    private Clock clock;

    public RdbFileParser(BufferedInputStream file, Clock clock) {
        reader = new RdbParsePrimitives(file);
        this.clock = clock;
    }

    public OpCode initDB() throws IOException {
        reader.readHeader();
        String dbVersion = new String(reader.readChars(4));
        System.out.println("DB Version: " + dbVersion);
        return reader.readCode();
    }

    public OpCode selectDB(Map<String, StoredData> dbData) throws IOException {
        int dbNumber = reader.readValue(reader.read()).getValue();
        System.out.println("Select DB: " + dbNumber);
        int next = reader.read();
        OpCode nextCode = OpCode.fromCode(next);
        if (nextCode == OpCode.RESIZEDB) {
            skipResizeDb();
            next = reader.read(); // read the valueType
            nextCode = null;
        }
        while (nextCode == null
                || nextCode == OpCode.EXPIRETIME
                || nextCode == OpCode.EXPIRETIMEMS) {
            System.out.println("Next code at file index: " + nextCode + " " + reader.file.readCount);
            Long expiryTime = null;
            if (nextCode == OpCode.EXPIRETIME) {
                // read 4 bytes for expiry time in seconds
                expiryTime = reader.readInt() * 1000L;
                next = reader.read();
            } else if (nextCode == OpCode.EXPIRETIMEMS) {
                // read 8 bytes for expiry time in milliseconds
                expiryTime = reader.readLong();
                next = reader.read();
            }

            int valueType = next;
            next = reader.read();
            // 0 = String Encoding
            // 1 = List Encoding
            // 2 = Set Encoding
            // 3 = Sorted Set Encoding
            // 4 = Hash Encoding
            // 9 = Zipmap Encoding
            // 10 = Ziplist Encoding
            // 11 = Intset Encoding
            // 12 = Sorted Set in Ziplist Encoding
            // 13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
            // 14 = List in Quicklist encoding (Introduced in RDB version 7)
            EncodedValue keyLength = reader.readValue(next);
            byte[] keyBytes;
            if (!keyLength.isString()) {
                // length encoded string
                keyBytes = reader.readNBytes(keyLength.getValue());
            } else {
                keyBytes = keyLength.getString().getBytes();
            }
            System.out.println("Read key: " + new String(keyBytes) + " with expiry: " + expiryTime);
            next = reader.read();
            EncodedValue value = reader.readValue(next);
            if (valueType != 0 || !value.isInt()) {
                throw new IllegalArgumentException(String.format(
                        "Expected value should be a length prefix string, valueType: %d, value: %s",
                        valueType,
                        value));
            }
            byte[] valueBytes = reader.readNBytes(value.getValue());
            long now = clock.millis();
            Long ttlMillis = null;
            if (expiryTime != null) {
                ttlMillis = expiryTime - now;
            }
            StoredData valueData = new StoredData(valueBytes, 0L, ttlMillis);
            dbData.put(new String(keyBytes), valueData);

            next = reader.read();
            nextCode = OpCode.fromCode(next);
        }
        return nextCode;
    }

    public OpCode skipAux() throws IOException {
        OpCode nextCode = null;
        while (nextCode == null || nextCode == OpCode.AUX) {
            nextCode = OpCode.fromCode(reader.read());
        }
        return nextCode;
    }

    public void skipResizeDb() throws IOException {
        EncodedValue dbSize = reader.readValue(reader.read());
        EncodedValue expirySize = reader.readValue(reader.read());
        System.out.println(String.format("Skipping RESIZEDB dbSize: %s, expirySize: %s",
                dbSize.getValue(), expirySize.getValue()));
    }
}