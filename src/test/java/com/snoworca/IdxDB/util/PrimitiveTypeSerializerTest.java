package com.snoworca.IdxDB.util;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class PrimitiveTypeSerializerTest {

    String randomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    @Test
    void serializeTest() {
        Random random = new Random(System.currentTimeMillis());

        boolean vBoolean = random.nextBoolean();
        byte vByte = (byte) random.nextInt();
        short vShort = (short) random.nextInt();
        char vChar = (char) random.nextInt();
        int vInt = random.nextInt();
        long vLong = random.nextLong();
        float vFloat = random.nextFloat();
        double vDouble = random.nextDouble();
        String vString = randomString(random.nextInt(100) + 10);

        boolean[] vBooleanArray = new boolean[random.nextInt(100) + 10];
        for(int i = 0; i < vBooleanArray.length; i++) {
            vBooleanArray[i] = random.nextBoolean();
        }
        byte[] vByteArray = new byte[random.nextInt(100) + 10];
        for(int i = 0; i < vByteArray.length; i++) {
            vByteArray[i] = (byte) random.nextInt();
        }
        short[] vShortArray = new short[random.nextInt(100) + 10];
        for(int i = 0; i < vShortArray.length; i++) {
            vShortArray[i] = (short) random.nextInt();
        }
        char[] vCharArray = new char[random.nextInt(100) + 10];
        for(int i = 0; i < vCharArray.length; i++) {
            vCharArray[i] = (char) random.nextInt();
        }
        int[] vIntArray = new int[random.nextInt(100) + 10];
        for(int i = 0; i < vIntArray.length; i++) {
            vIntArray[i] = random.nextInt();
        }
        long[] vLongArray = new long[random.nextInt(100) + 10];
        for(int i = 0; i < vLongArray.length; i++) {
            vLongArray[i] = random.nextLong();
        }
        float[] vFloatArray = new float[random.nextInt(100) + 10];
        for(int i = 0; i < vFloatArray.length; i++) {
            vFloatArray[i] = random.nextFloat();
        }
        double[] vDoubleArray = new double[random.nextInt(100) + 10];
        for(int i = 0; i < vDoubleArray.length; i++) {
            vDoubleArray[i] = random.nextDouble();
        }
        String[] vStringArray = new String[random.nextInt(100) + 10];
        for(int i = 0; i < vStringArray.length; i++) {
            vStringArray[i] = randomString(random.nextInt(100) + 10);
        }


        byte[] vBooleanByte = PrimitiveTypeSerializer.serializeAnything(vBoolean);
        byte[] vByteByte = PrimitiveTypeSerializer.serializeAnything(vByte);
        byte[] vShortByte = PrimitiveTypeSerializer.serializeAnything(vShort);
        byte[] vCharByte = PrimitiveTypeSerializer.serializeAnything(vChar);
        byte[] vIntByte = PrimitiveTypeSerializer.serializeAnything(vInt);
        byte[] vLongByte = PrimitiveTypeSerializer.serializeAnything(vLong);
        byte[] vFloatByte = PrimitiveTypeSerializer.serializeAnything(vFloat);
        byte[] vDoubleByte = PrimitiveTypeSerializer.serializeAnything(vDouble);
        byte[] vStringByte = PrimitiveTypeSerializer.serializeAnything(vString);

        byte[] vBooleanByteArray = PrimitiveTypeSerializer.serializeAnything(vBooleanArray);
        byte[] vByteArrayArray = PrimitiveTypeSerializer.serializeAnything(vByteArray);
        byte[] vShortArrayArray = PrimitiveTypeSerializer.serializeAnything(vShortArray);
        byte[] vCharArrayArray = PrimitiveTypeSerializer.serializeAnything(vCharArray);
        byte[] vIntArrayArray = PrimitiveTypeSerializer.serializeAnything(vIntArray);
        byte[] vLongArrayArray = PrimitiveTypeSerializer.serializeAnything(vLongArray);
        byte[] vFloatArrayArray = PrimitiveTypeSerializer.serializeAnything(vFloatArray);
        byte[] vDoubleArrayArray = PrimitiveTypeSerializer.serializeAnything(vDoubleArray);
        byte[] vStringArrayArray = PrimitiveTypeSerializer.serializeAnything(vStringArray);

        assertEquals(vBoolean, PrimitiveTypeSerializer.deserializeAnything(vBooleanByte));
        assertEquals(vByte, PrimitiveTypeSerializer.deserializeAnything(vByteByte));
        assertEquals(vShort, PrimitiveTypeSerializer.deserializeAnything(vShortByte));
        assertEquals(vChar, PrimitiveTypeSerializer.deserializeAnything(vCharByte));
        assertEquals(vInt, PrimitiveTypeSerializer.deserializeAnything(vIntByte));
        assertEquals(vLong, PrimitiveTypeSerializer.deserializeAnything(vLongByte));
        assertEquals(vFloat, PrimitiveTypeSerializer.deserializeAnything(vFloatByte));
        assertEquals(vDouble, PrimitiveTypeSerializer.deserializeAnything(vDoubleByte));
        assertEquals(vString, PrimitiveTypeSerializer.deserializeAnything(vStringByte));

        assertArrayEquals(vBooleanArray, (boolean[]) PrimitiveTypeSerializer.deserializeAnything(vBooleanByteArray));
        assertArrayEquals(vByteArray, (byte[]) PrimitiveTypeSerializer.deserializeAnything(vByteArrayArray));
        assertArrayEquals(vShortArray, (short[]) PrimitiveTypeSerializer.deserializeAnything(vShortArrayArray));
        assertArrayEquals(vCharArray, (char[]) PrimitiveTypeSerializer.deserializeAnything(vCharArrayArray));
        assertArrayEquals(vIntArray, (int[]) PrimitiveTypeSerializer.deserializeAnything(vIntArrayArray));
        assertArrayEquals(vLongArray, (long[]) PrimitiveTypeSerializer.deserializeAnything(vLongArrayArray));
        assertArrayEquals(vFloatArray, (float[]) PrimitiveTypeSerializer.deserializeAnything(vFloatArrayArray));
        assertArrayEquals(vDoubleArray, (double[]) PrimitiveTypeSerializer.deserializeAnything(vDoubleArrayArray));
        assertArrayEquals(vStringArray, (String[]) PrimitiveTypeSerializer.deserializeAnything(vStringArrayArray));


        assertArrayEquals(vStringArray, (String[]) PrimitiveTypeSerializer.deserializeArrayString(vStringArrayArray, 1));







    }

}