package org.baylight.redis.rdb;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.StreamBuilder;
import org.baylight.redis.rdb.EncodedValue.Code;
import org.junit.jupiter.api.Test;

public class RdbParsePrimitivesTest implements WithAssertions {

    @Test
    void testReadChars() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIS");
        RdbParsePrimitives parser = new RdbParsePrimitives(builder.build());
        assertThat(parser.readChars(3)).isEqualTo("RED".toCharArray());
        assertThat(parser.readChars(2)).isEqualTo("IS".toCharArray());
    }

    @Test
    void testReadHeaderTrue() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIS");
        RdbParsePrimitives parser = new RdbParsePrimitives(builder.build());
        assertThat(parser.readHeader()).isTrue();
    }

    @Test
    void testReadHeaderFalse() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIX");
        RdbParsePrimitives parser = new RdbParsePrimitives(builder.build());
        assertThat(parser.readHeader()).isFalse();
    }

    @Test
    void testReadCode() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write("REDIS");
        builder.write(OpCode.EXPIRETIME.getCode());
        builder.write(OpCode.EXPIRETIMEMS.getCode());
        builder.write(OpCode.AUX.getCode());
        builder.write(OpCode.SELECTDB.getCode());
        builder.write(OpCode.RESIZEDB.getCode());
        builder.write(OpCode.EOF.getCode());
        RdbParsePrimitives parser = new RdbParsePrimitives(builder.build());
        parser.readHeader();
        assertThat(parser.readCode()).isEqualTo(OpCode.EXPIRETIME);
        assertThat(parser.readCode()).isEqualTo(OpCode.EXPIRETIMEMS);
        assertThat(parser.readCode()).isEqualTo(OpCode.AUX);
        assertThat(parser.readCode()).isEqualTo(OpCode.SELECTDB);
        assertThat(parser.readCode()).isEqualTo(OpCode.RESIZEDB);
        assertThat(parser.readCode()).isEqualTo(OpCode.EOF);
    }

    @Test
    void testReadValue() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x39); // 2 bits 00
        builder.write(new byte[] { (byte) (1 << 6) + 9, 0x5 });
        builder.write(new byte[] { (byte) (2 << 6) + 11, 0xF, 0x5, 0x6, 0x7 });
        BufferedInputStream file = builder.build();
        RdbParsePrimitives parser = new RdbParsePrimitives(file);
        assertThat(parser.readValue(file.read())).isEqualTo(new EncodedValue(Code.INT6, 0x39));
        assertThat(parser.readValue(file.read())).isEqualTo(new EncodedValue(Code.INT14, 0x905));
        assertThat(parser.readValue(file.read())).isEqualTo(new EncodedValue(Code.INT32, 0xF050607));
    }

    @Test
    void testReadNBytes() throws Exception {
        StreamBuilder builder = new StreamBuilder();
        builder.write(0x39); // 2 bits 00
        byte[] bytes = new byte[0x39];
        Arrays.fill(bytes, (byte) 0x05);
        builder.write(bytes);
        BufferedInputStream file = builder.build();
        RdbParsePrimitives parser = new RdbParsePrimitives(file);
        assertThat(parser.readValue(file.read())).isEqualTo(new EncodedValue(Code.INT6, 0x39));
        byte[] expected = new byte[0x30];
        Arrays.fill(expected, (byte) 0x05);
        assertThat(parser.readNBytes(0x30)).isEqualTo(expected);
        assertThatThrownBy(() -> parser.readNBytes(0xA)).isInstanceOf(IOException.class)
                .hasMessage("Unexpected end of file");
    }
}
