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
        int value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value = (value << 8) + (bytes[i] & 0xFF);
        }
        return value;
    }

    public static int decodeInt(int... bytes) {
        // decode bytes to an integer
        int value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value = (value << 8) + (bytes[i] & 0xFF);
        }
        return value;
    }

    public static long decodeLong(byte... bytes) {
        // decode bytes to an long
        System.out.println(String.format("decodeLong: %s", formatBytesArray(bytes)));
        long value = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (i == 0) { // hack for unsigned long support
                value = bytes[i] & 0x7F;
            } else {
                value = (value << 8) + (bytes[i] & 0xFF);
            }
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
