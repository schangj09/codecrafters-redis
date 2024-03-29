package org.baylight.redis.rdb;

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.StoredData;
import org.baylight.redis.StreamBuilder;
import org.baylight.redis.TestConstants;
import org.junit.jupiter.api.Test;

public class RdbFileParserTest implements WithAssertions, TestConstants {

    @Test
    void testInit() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIS");
        builder.write("0001");
        builder.write(0xFE); // select db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);
        assertThat(parser.initDB()).isEqualTo(OpCode.SELECTDB);
    }

    @Test
    void testSelectDB() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        builder.write(0xFB); // resize db
        builder.write(0x02); // db hash table size
        builder.write(0x00); // expiry hash table size
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key2");
        builder.write(4);
        builder.write("v002");
        builder.write(0xFE); // next db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);

        Map<String, StoredData> dbData = new HashMap<>();

        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
                "key1", new StoredData("v01".getBytes(), CLOCK_MILLIS, null),
                "key2", new StoredData("v002".getBytes(), CLOCK_MILLIS, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    @Test
    void testSelectDBWithExpiry() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key0");
        builder.write(3);
        builder.write("v00");
        builder.write(0xFD); // expiry in seconds
        builder.write(encode(CLOCK_MILLIS / 1000 + 500, 4)); // 4 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        builder.write(0xFC); // expiry in milliseconds
        builder.write(encode(CLOCK_MILLIS + 0x7F0000000L, 8)); // 8 byte int (would be negative if
                                                               // not long)
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key2");
        builder.write(4);
        builder.write("v002");
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key3");
        builder.write(4);
        builder.write("v003");
        builder.write(0xFE); // next db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);

        Map<String, StoredData> dbData = new HashMap<>();

        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
                "key0", new StoredData("v00".getBytes(), CLOCK_MILLIS, null),
                "key1", new StoredData("v01".getBytes(), CLOCK_MILLIS, 500000L),
                "key2", new StoredData("v002".getBytes(), CLOCK_MILLIS, 0x7F0000000L),
                "key3", new StoredData("v003".getBytes(), CLOCK_MILLIS, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    @Test
    void testSelectDBResizeAndExpiry() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        builder.write(0xFB); // resize db
        builder.write(0x02); // db hash table size
        builder.write(0x01); // expiry hash table size
        // key0
        builder.write(0xFD); // expiry in seconds
        builder.write(encode(CLOCK_MILLIS / 1000 + 500, 4)); // 4 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key0");
        builder.write(3);
        builder.write("v00");
        // key1
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        // key2
        builder.write(0xFC); // expiry in milliseconds
        builder.write(encode(1640995200000L, 8)); // 8 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key2");
        builder.write(4);
        builder.write("v002");

        builder.write(0xFE); // next db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);

        Map<String, StoredData> dbData = new HashMap<>();

        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
                "key0", new StoredData("v00".getBytes(), CLOCK_MILLIS, 500000L),
                "key1", new StoredData("v01".getBytes(), CLOCK_MILLIS, null),
                "key2", new StoredData("v002".getBytes(), CLOCK_MILLIS, 1640995200000L - CLOCK_MILLIS));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    @Test
    void testSelectDBWithExpiryInPast() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        // key0
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key0");
        builder.write(3);
        builder.write("v00");
        // key1
        builder.write(0xFD); // expiry in seconds
        builder.write(encode(CLOCK_MILLIS / 1000 - 50, 4)); // 4 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        // key2
        builder.write(0xFC); // expiry in milliseconds
        builder.write(encode(CLOCK_MILLIS, 8)); // 8 byte int equal to current time
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key2");
        builder.write(4);
        builder.write("v002");
        // key3
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key3");
        builder.write(4);
        builder.write("v003");
        builder.write(0xFE); // next db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);

        Map<String, StoredData> dbData = new HashMap<>();

        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
                "key0", new StoredData("v00".getBytes(), CLOCK_MILLIS, null),
                "key3", new StoredData("v003".getBytes(), CLOCK_MILLIS, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    byte[] encode(long val, int byteCount) {
        byte[] enc = new byte[byteCount];
        for (int j = 0; j < byteCount; j++) {
            int shift = (j * 8);
            enc[j] = (byte) ((val >> shift) & 0xFF);
        }
        return enc;
    }

}