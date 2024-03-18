package org.baylight.redis.rdb;

import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.StoredData;
import org.baylight.redis.StreamBuilder;
import org.junit.jupiter.api.Test;

public class RdbFileParserTest implements WithAssertions {

    @Test
    void testInit() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIS");
        builder.write("0001");
        builder.write(0xFE); // select db

        RdbFileParser parser = new RdbFileParser(builder.build());
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

        RdbFileParser parser = new RdbFileParser(builder.build());

        Map<String, StoredData> dbData = new HashMap<>();
        
        assertThat(parser.selectDB(dbData)).isEqualTo(OpCode.SELECTDB);

        Map<String, StoredData> expectedResult = Map.of(
            "key1", new StoredData("v01".getBytes(), 0, null),
            "key2", new StoredData("v002".getBytes(), 0, null)
        );
        assertThat(dbData).isEqualTo(expectedResult);
    }

}