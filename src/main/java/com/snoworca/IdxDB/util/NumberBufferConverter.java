package com.snoworca.IdxDB.util;

public class NumberBufferConverter {




    public static byte[] fromChar(char value) {
        byte[] buffer = new byte[2];
        fromChar(value, buffer, 0);
        return buffer;
    }

    public static void fromChar(char value, byte[] byteArray, int offset) {
        fromShort((short)value, byteArray, offset);
    }

    public static short toChar(byte bytes[]) {
        return toChar(bytes, 0);
    }

    public static short toChar(byte bytes[], int offset) {
        return toShort(bytes, offset);
    }




    public static byte[] fromShort(short value) {
        byte[] buffer = new byte[2];
        fromShort(value, buffer, 0);
        return buffer;
    }

    public static void fromShort(short value, byte[] byteArray, int offset) {
        byteArray[offset] = (byte)(value >> 8);
        byteArray[offset + 1] = (byte)(value);
    }

    public static short toShort(byte bytes[]) {
        return toShort(bytes, 0);
    }

    public static short toShort(byte bytes[], int offset) {
        return (short)((((short)bytes[offset] & 0xff) << 8) |
                        (((short)bytes[offset + 1] & 0xff)));
    }


    public static byte[] fromFloat(float value) {
        byte[] byteArray = new byte[4];
        fromFloat(value, byteArray, 0);
        return byteArray;
    }

    public static void fromFloat(float value, byte[] byteArray, int offset) {
        fromInt(Float.floatToIntBits(value), byteArray, offset);
    }

    public static float toFloat(byte bytes[]) {
        return toFloat(bytes, 0);

    }

    public static float toFloat(byte bytes[], int offset) {
        int value = toInt(bytes, offset);
        return Float.intBitsToFloat(value);
    }


    public static byte[] fromInt(int value) {
        byte[] byteArray = new byte[4];
        fromInt(value, byteArray, 0);
        return byteArray;
    }

    public static  void fromInt(int value, byte[] byteArray, int offset) {
        byteArray[offset] = (byte)(value >> 24);
        byteArray[offset + 1] = (byte)(value >> 16);
        byteArray[offset + 2] = (byte)(value >> 8);
        byteArray[offset + 3] = (byte)(value);
    }



    public static int toInt(byte bytes[]) {
        return toInt(bytes, 0);
    }

    public static int toInt(byte bytes[], int offset) {
        return ((((int)bytes[offset] & 0xff) << 24) |
                (((int)bytes[offset + 1] & 0xff) << 16) |
                (((int)bytes[offset + 2] & 0xff) << 8) |
                (((int)bytes[offset + 3] & 0xff)));
    }

    public static byte[] fromLong(long value) {
        byte[] buffer = new byte[8];
        fromLong(value, buffer, 0);
        return buffer;
    }

    public static void fromLong(long value, byte[] byteArray, int offset) {
        byteArray[offset] = (byte)(value >> 56);
        byteArray[offset + 1] = (byte)(value >> 48);
        byteArray[offset + 2] = (byte)(value >> 40);
        byteArray[offset + 3] = (byte)(value >> 32);

        byteArray[offset + 4] = (byte)(value >> 24);
        byteArray[offset + 5] = (byte)(value >> 16);
        byteArray[offset + 6] = (byte)(value >> 8);
        byteArray[offset + 7] = (byte)(value);
    }


    public static long toLong(byte bytes[]) {
        return toLong(bytes, 0);
    }



    public static long toLong(byte bytes[], int offset) {
        return ((((long)bytes[offset] & 0xff) << 56) |
                (((long)bytes[offset + 1] & 0xff) << 48) |
                (((long)bytes[offset + 2] & 0xff) << 40) |
                (((long)bytes[offset + 3] & 0xff) << 32) |

                (((long)bytes[offset + 4] & 0xff) << 24) |
                (((long)bytes[offset + 5] & 0xff) << 16) |
                (((long)bytes[offset + 6] & 0xff) << 8) |
                (((long)bytes[offset + 7] & 0xff)));
    }


    public static byte[] fromDouble(double value) {
        byte[] byteArray = new byte[4];
        fromDouble(value, byteArray, 0);
        return byteArray;
    }

    public static void fromDouble(double value, byte[] byteArray, int offset) {
        fromLong(Double.doubleToLongBits(value), byteArray, offset);
    }

    public static double toDouble(byte bytes[]) {
        return toFloat(bytes, 0);

    }

    public static double toDouble(byte bytes[], int offset) {
        long value = toLong(bytes, offset);
        return Double.longBitsToDouble(value);
    }


}
