package org.baylight.redis.rdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class EncodedValue {
    enum Code {
        INT6(0x0), INT14(0x40), INT32(0x80), INTSTRING8(0xC0), INTSTRING16(0xC1), INTSTRING32(0xC2),
        COMPRESSED_STRING(0xC3);

        int code;

        Code(int code) {
            this.code = code;
        }

        static Code fromByte(int firstByte) {
            switch (firstByte >> 6) {
            case 0:
                return INT6;
            case 1:
                return INT14;
            case 2:
                return INT32;
            case 3: {
                return switch (firstByte) {
                case 0xC0 -> INTSTRING8;
                case 0xC1 -> INTSTRING16;
                case 0xC2 -> INTSTRING32;
                case 0xC3 -> COMPRESSED_STRING;
                default -> null;
                };

            }
            default:
                return null;
            }
        }
    }

    Code code;
    int value;
    String string;

    public EncodedValue(Code code, int value) {
        this.code = code;
        this.value = value;
    }

    public EncodedValue(Code code, String string) {
        this.code = code;
        this.string = string;
    }

    public boolean isInt() {
        return code == Code.INT6 || code == Code.INT14 || code == Code.INT32;
    }

    public int getValue() {
        if (!isInt()) {
            throw new IllegalStateException("Expected an int value");
        }
        return value;
    }

    public boolean isString() {
        return code == Code.INTSTRING8 || code == Code.INTSTRING16 || code == Code.INTSTRING32;
    }

    public String getString() {
        return string;
    }

    public static EncodedValue parseValue(int first, InputStream in) throws IOException {
        Code code = Code.fromByte(first);
        switch (code) {
        case INT6:
            return new EncodedValue(code, first & 0x3F);
        case INT14:
            return new EncodedValue(code, ByteUtils.decodeInt((first & 0x3F), in.read()));
        case INT32:
            // read 4 bytes from in as an encoded 32 bit integer
            byte[] bytes = new byte[4];
            int n = in.readNBytes(bytes, 0, 4);
            if (n != 4) {
                throw new IllegalArgumentException("Parse error: expected 4 byte integer");
            }
            return new EncodedValue(code, ByteUtils.decodeInt(bytes));
        case INTSTRING8:
        case INTSTRING16:
        case INTSTRING32:
        case COMPRESSED_STRING:
            throw new UnsupportedOperationException("Unsupported value code: " + code.name());
        default:
            throw new IllegalArgumentException("Invalid byte for value code: " + first);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, value, string);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof EncodedValue))
            return false;
        EncodedValue other = (EncodedValue) obj;
        return code == other.code && value == other.value && Objects.equals(string, other.string);
    }

    @Override
    public String toString() {
        return "EncodedValue [code=" + code + ", value=" + value + ", string=" + string + "]";
    }

}
