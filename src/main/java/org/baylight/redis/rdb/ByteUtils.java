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

    public static int decodeLong(byte... bytes) {
        // decode bytes to an integer
        int value = 0;
        for (int i = 0; i < bytes.length; i++) {
            value = (value << 8) + (bytes[i] & 0xFF);
        }
        return value;
    }

}
