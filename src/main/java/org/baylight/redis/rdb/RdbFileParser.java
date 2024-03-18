package org.baylight.redis.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Map;

import org.baylight.redis.StoredData;

public class RdbFileParser {

    private BufferedInputStream file;
    private final RdbParsePrimitives primitiveParser;

    public RdbFileParser(BufferedInputStream file) {
        this.file = file;
        primitiveParser = new RdbParsePrimitives(file);
    }

    public OpCode initDB() throws IOException {
        primitiveParser.readHeader();
        String dbVersion = new String(primitiveParser.readChars(4));
        System.out.println("DB Version: " + dbVersion);
        return primitiveParser.readCode();
    }

    public OpCode selectDB(Map<String, StoredData> dbData) throws IOException {
        int dbNumber = primitiveParser.readValue(file.read()).getValue();
        System.out.println("Select DB: " + dbNumber);
        int next = file.read();
        OpCode nextCode = OpCode.fromCode(next);
        if (nextCode == OpCode.RESIZEDB) {
            throw new UnsupportedOperationException("RESIZEDB not supported yet");
        }
        while (nextCode == null || nextCode == OpCode.EXPIRETIME
                || nextCode == OpCode.EXPIRETIMEMS) {
            if (nextCode == OpCode.EXPIRETIME || nextCode == OpCode.EXPIRETIMEMS) {
                throw new UnsupportedOperationException("Expire time not supported yet");
            }
            int valueType = next;
            next = file.read();
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
            EncodedValue keyLength = primitiveParser.readValue(next);
            byte[] keyBytes;
            if (!keyLength.isString()) {
                // length encoded string
                keyBytes = primitiveParser.readNBytes(keyLength.getValue());
            } else {
                keyBytes = keyLength.getString().getBytes();
            }
            next = file.read();
            EncodedValue value = primitiveParser.readValue(next);
            if (valueType != 0 || !value.isInt()) {
                throw new IllegalArgumentException(String.format("expected value is a length prefix string, type: %d, value: %s", valueType, value));
            }
            byte[] valueBytes = primitiveParser.readNBytes(value.getValue());
            StoredData valueData = new StoredData(valueBytes, 0L, null);
            dbData.put(new String(keyBytes), valueData);

            next = file.read();
            nextCode = OpCode.fromCode(next);
        }
        return nextCode;
    }

}