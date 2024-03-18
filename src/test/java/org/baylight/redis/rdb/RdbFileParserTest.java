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
                "key1", new StoredData("v01".getBytes(), 0, null),
                "key2", new StoredData("v002".getBytes(), 0, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    @Test
    void testSelectDBWithExpiry() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        builder.write(0xFD); // expiry in seconds
        builder.write(encode(CLOCK_MILLIS/1000 + 500, 4)); // 4 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        builder.write(0xFC); // expiry in milliseconds
        builder.write(encode(CLOCK_MILLIS + 888888, 8)); // 8 byte int
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
            "key1", new StoredData("v01".getBytes(), 0, 500000L),
                "key2", new StoredData("v002".getBytes(), 0, 888888L),
                "key3", new StoredData("v003".getBytes(), 0, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    @Test
    void testSelectDBFirstWithExpiry() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x09); // db number
        builder.write(0xFD); // expiry in seconds
        builder.write(encode(CLOCK_MILLIS/1000 + 500, 4)); // 4 byte int
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key0");
        builder.write(3);
        builder.write("v00");
        builder.write(0x00); // value type string
        builder.write(4);
        builder.write("key1");
        builder.write(3);
        builder.write("v01");
        builder.write(0xFE); // next db

        RdbFileParser parser = new RdbFileParser(builder.build(), FIXED_CLOCK);

        Map<String, StoredData> dbData = new HashMap<>();

        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
            "key0", new StoredData("v00".getBytes(), 0, 500000L),
            "key1", new StoredData("v01".getBytes(), 0, null));
        assertThat(dbData).isEqualTo(expectedResult);
    }

    byte[] encode(long i, int byteCount) {
        byte[] enc = new byte[byteCount];
        for (int j = 0; j < byteCount; j++) {
            int shift = (byteCount - 1 - j) * 8;
            enc[j] = (byte) ((i >> shift) & 0xFF);
        }
        return enc;
    }

}