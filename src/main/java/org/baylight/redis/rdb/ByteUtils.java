package org.baylight.redis.rdb;

public class ByteUtils {

    static boolean compareToString(char[] bytes, String s) {
        // compare the bytes to the string
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != s.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    public static int decodeInt(byte... bytes) {
        // decode bytes to an integer
        // hack for unsigned int support, chop off the most significant bit
        return (int) (decodeLong(bytes) & 0x7FFF);
    }

    public static int decodeInt(int... bytes) {
        // decode bytes to an integer - least significant bytes first
        long value = 0;
        for (int i = 0; i < bytes.length && i < Integer.BYTES; i++) {
            byte byteVal = (byte) (bytes[i] & 0xFF);
            int shift = i * 8;
            value |= ((long) byteVal << shift) & ((long) 0xFF << shift);
        }
        // hack for unsigned long support, chop off the most significant bit
        return (int)(value & 0x7FFF);
    }

    public static long decodeLong(byte... bytes) {
        // decode bytes to a long - least significant bytes first
        System.out.println(String.format("decodeLong: %s", formatBytesArray(bytes)));
        long value = 0;
        for (int i = 0; i < bytes.length && i < Long.BYTES; i++) {
            byte byteVal = (byte) (bytes[i] & 0xFF);
            if (i == Long.BYTES - 1) { // hack for unsigned long support, chop off the most
                                       // significant bit
                byteVal = (byte) (bytes[i] & 0x7F);
            }
            int shift = i * 8;
            value |= ((long) byteVal << shift) & ((long) 0xFF << shift);
        }
        return value;
    }

    private static String formatBytesArray(byte[] bytes) {
        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; i < bytes.length; i++) {
            b.append(String.format("%X", i == 0 ? bytes[i] & 0x7F : bytes[i] & 0xFF));
            if (i == bytes.length - 1)
                b.append(']');
            else
                b.append(", ");
        }
        return b.toString();
    }

}
