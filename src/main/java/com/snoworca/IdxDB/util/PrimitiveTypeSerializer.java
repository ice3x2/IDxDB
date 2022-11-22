package com.snoworca.IdxDB.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;

public class PrimitiveTypeSerializer {

    public final static byte TYPE_NULL = 0;
    public final static byte TYPE_BYTE = 1;
    public final static byte TYPE_BOOLEAN = 2;
    public final static byte TYPE_SHORT = 3;
    public final static byte TYPE_CHAR = 4;
    public final static byte TYPE_INT = 5;
    public final static byte TYPE_LONG = 6;
    public final static byte TYPE_FLOAT = 7;
    public final static byte TYPE_DOUBLE = 8;
    public final static byte TYPE_STRING = 9;
    public final static byte TYPE_BYTE_ARRAY = 10;
    public final static byte TYPE_BOOLEAN_ARRAY = 11;
    public final static byte TYPE_SHORT_ARRAY = 12;
    public final static byte TYPE_CHAR_ARRAY = 13;
    public final static byte TYPE_INT_ARRAY = 14;
    public final static byte TYPE_LONG_ARRAY = 15;
    public final static byte TYPE_FLOAT_ARRAY = 16;
    public final static byte TYPE_DOUBLE_ARRAY = 17;
    public final static byte TYPE_STRING_ARRAY = 18;

    public static Object deserializeAnything(byte[] buffer) {
        return deserializeAnything(buffer, 0);
    }

    public static Object deserializeAnything(byte[] buffer, int offset) {
        int type = buffer[offset];
        if(type == TYPE_NULL) {
            return null;
        }
        else if(type == TYPE_BYTE) {
            return buffer[offset + 1];
        }
        else if(type == TYPE_BOOLEAN) {
            return buffer[offset + 1] == 1;
        }
        else if(type == TYPE_SHORT) {
            return deserializeShort(buffer, offset + 1);
        }
        else if(type == TYPE_CHAR) {
            return deserializeChar(buffer, offset + 1);
        }
        else if(type == TYPE_INT) {
            return deserializeInt(buffer, offset + 1);
        }
        else if(type == TYPE_LONG) {
            return deserializeLong(buffer, offset + 1);
        }
        else if(type == TYPE_FLOAT) {
            return deserializeFloat(buffer, offset + 1);
        }
        else if(type == TYPE_DOUBLE) {
            return deserializeDouble(buffer, offset + 1);
        }
        else if(type == TYPE_STRING) {
            return deserializeString(buffer, offset + 1);
        }
        else if(type == TYPE_BYTE_ARRAY) {
            return deserializeArray(buffer, offset + 1);
        }
        else if(type == TYPE_BOOLEAN_ARRAY) {
            return deserializeArrayBoolean(buffer, offset + 1);
        }
        else if(type == TYPE_SHORT_ARRAY) {
            return deserializeArrayShort(buffer, offset + 1);
        }
        else if(type == TYPE_CHAR_ARRAY) {
            return deserializeArrayChar(buffer, offset + 1);
        }
        else if(type == TYPE_INT_ARRAY) {
            return deserializeIntArray(buffer, offset + 1);
        }
        else if(type == TYPE_LONG_ARRAY) {
            return deserializeArrayLong(buffer, offset + 1);
        }
        else if(type == TYPE_FLOAT_ARRAY) {
            return deserializeArrayFloat(buffer, offset + 1);
        }
        else if(type == TYPE_DOUBLE_ARRAY) {
            return deserializeArrayDouble(buffer, offset + 1);
        }
        else if(type == TYPE_STRING_ARRAY) {
            return deserializeArrayString(buffer, offset + 1);
        }
        else {
            throw new RuntimeException("Unknown type: " + type);
        }
    }


    public static byte[] serializeAnything(Object obj) {
        return serializeAnything(obj, false);
    }

    public static byte[] serializeAnything(Object obj, boolean notPrimitiveToString) {
        if(obj == null) {
            return new byte[]{TYPE_NULL};
        } else if(obj instanceof Byte) {
            byte[] bytes = new byte[2];
            bytes[0] = TYPE_BYTE;
            bytes[1] = (Byte) obj;
            return bytes;
        } else if(obj instanceof Boolean) {
            byte[] bytes = new byte[2];
            bytes[0] = TYPE_BOOLEAN;
            bytes[1] = (byte) (((Boolean) obj) ? 1 : 0);
            return bytes;
        } else if(obj instanceof Short) {
            byte[] bytes = new byte[3];
            bytes[0] = TYPE_SHORT;
            serialize((Short) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof Character) {
            byte[] bytes = new byte[3];
            bytes[0] = TYPE_CHAR;
            serialize((Character) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof Integer) {
            byte[] bytes = new byte[5];
            bytes[0] = TYPE_INT;
            serialize((Integer) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof Long) {
            byte[] bytes = new byte[9];
            bytes[0] = TYPE_LONG;
            serialize((Long) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof Float) {
            byte[] bytes = new byte[5];
            bytes[0] = TYPE_FLOAT;
            serialize((Float) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof Double) {
            byte[] bytes = new byte[9];
            bytes[0] = TYPE_DOUBLE;
            serialize((Double) obj, bytes, 1);
            return bytes;
        } else if(obj instanceof String) {
            byte[] bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
            byte[] result = new byte[bytes.length + 5];
            result[0] = TYPE_STRING;
            serialize(bytes.length, result, 1);
            System.arraycopy(bytes, 0, result, 5, bytes.length);
            return result;
        }
        else if(obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            byte[] result = new byte[bytes.length + 5];
            result[0] = TYPE_BYTE_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof boolean[]) {
            boolean[] bytes = (boolean[]) obj;
            byte[] result = new byte[bytes.length + 5];
            result[0] = TYPE_BOOLEAN_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof short[]) {
            short[] bytes = (short[]) obj;
            byte[] result = new byte[bytes.length * 2 + 5];
            result[0] = TYPE_SHORT_ARRAY;
            serializeArray((short[])obj, result, 1);
            return result;
        } else if(obj instanceof char[]) {
            char[] bytes = (char[]) obj;
            byte[] result = new byte[bytes.length * 2 + 5];
            result[0] = TYPE_CHAR_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof int[]) {
            int[] bytes = (int[]) obj;
            byte[] result = new byte[bytes.length * 4 + 5];
            result[0] = TYPE_INT_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof long[]) {
            long[] bytes = (long[]) obj;
            byte[] result = new byte[bytes.length * 8 + 5];
            result[0] = TYPE_LONG_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof float[]) {
            float[] bytes = (float[]) obj;
            byte[] result = new byte[bytes.length * 4 + 5];
            result[0] = TYPE_FLOAT_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof double[]) {
            double[] bytes = (double[]) obj;
            byte[] result = new byte[bytes.length * 8 + 5];
            result[0] = TYPE_DOUBLE_ARRAY;
            serializeArray(bytes, result, 1);
            return result;
        } else if(obj instanceof String[]) {
            String[] stringArray = (String[]) obj;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                baos.write(TYPE_STRING_ARRAY);
                baos.write(serializeArray(stringArray));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return baos.toByteArray();
        } else if(notPrimitiveToString) {
            if(obj.getClass().isArray()) {
                int length = Array.getLength(obj);
                String[] stringArray = new String[length];
                for(int i = 0; i < length; i++) {
                    stringArray[i] = Array.get(obj, i).toString();
                }
                return serializeAnything(stringArray);
            } else {
                return serializeAnything(obj.toString());
            }
        }
        else {
            throw new RuntimeException("Unsupported type: " + obj.getClass().getName());
        }
    }



    public static byte[] serialize(int value) {
        return serialize(value, new byte[4], 0);
    }

    public static byte[] serialize(int value, byte[] bytes, int offset) {
        bytes[offset] = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) (value);
        return bytes;
    }

    public static int deserializeInt(byte[] bytes, int offset) {
        return (bytes[offset] << 24) + ((bytes[offset + 1] & 0xFF) << 16) + ((bytes[offset + 2] & 0xFF) << 8) + (bytes[offset + 3] & 0xFF);
    }

    public static int deserializeInt(byte[] bytes) {
        return deserializeInt(bytes, 0);
    }


    public static byte[] serializeArray(int[] value, byte[] bytes, int offset) {
        serialize(value.length, bytes, offset);
        offset += 4;
        for(int i = 0; i < value.length; i++) {
            serialize(value[i], bytes, offset);
            offset += 4;
        }
        return bytes;
    }

    public static int[] deserializeIntArray(byte[] bytes, int offset) {
        int length = deserializeInt(bytes, offset);
        offset += 4;
        int[] result = new int[length];
        for(int i = 0; i < length; i++) {
            result[i] = deserializeInt(bytes, offset);
            offset += 4;
        }
        return result;
    }


    public static byte[] serializeArray(byte[] array) {
        byte[] bytes = new byte[4 + array.length];
        serializeArray(array, bytes, 0);
        return bytes;
    }

    public static byte[] serializeArray(byte[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        System.arraycopy(array, 0, buffer, offset + 4, array.length);
        return buffer;
    }

    public static byte[] deserializeArray(byte[] bytes) {
        return deserializeArray(bytes, 0);
    }

    public static byte[] deserializeArray(byte[] bytes, int offset) {
        int length = (bytes[offset] & 0xFF) << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8 | (bytes[offset + 3] & 0xFF);
        byte[] array = new byte[length];
        System.arraycopy(bytes, offset + 4, array, 0, length);
        return array;
    }

    public static byte[] serialize(long value) {
        byte[] bytes = new byte[8];
        serialize(value, bytes, 0);
        return bytes;
    }

    public static byte[] serialize(long value, byte[] buffer, int offset) {
        buffer[offset] = (byte) (value >> 56);
        buffer[offset + 1] = (byte) (value >> 48);
        buffer[offset + 2] = (byte) (value >> 40);
        buffer[offset + 3] = (byte) (value >> 32);
        buffer[offset + 4] = (byte) (value >> 24);
        buffer[offset + 5] = (byte) (value >> 16);
        buffer[offset + 6] = (byte) (value >> 8);
        buffer[offset + 7] = (byte) (value);
        return buffer;
    }

    public static long deserializeLong(byte[] bytes) {
        return deserializeLong(bytes, 0);
    }

    public static long deserializeLong(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL) << 56 | (bytes[offset + 1] & 0xFFL) << 48 | (bytes[offset + 2] & 0xFFL) << 40 | (bytes[offset + 3] & 0xFFL) << 32 | (bytes[offset + 4] & 0xFFL) << 24 | (bytes[offset + 5] & 0xFFL) << 16 | (bytes[offset + 6] & 0xFFL) << 8 | (bytes[offset + 7] & 0xFFL);
    }

    public static byte[] serializeArray(long[] array) {
        byte[] bytes = new byte[4 + array.length * 8];
        return serializeArray(array, bytes, 0);
    }
    public static byte[] serializeArray(long[] array,byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        for (int i = 0; i < array.length; i++) {
            serialize(array[i], buffer, offset + 4 + i * 8);
        }
        return buffer;
    }


    public static long[] deserializeArrayLong(byte[] bytes) {
        return deserializeArrayLong(bytes, 0);
    }

    public static long[] deserializeArrayLong(byte[] bytes, int offset) {
        int length = deserializeInt(bytes, offset);
        long[] array = new long[length];
        for (int i = 0; i < length; i++) {
            array[i] = deserializeLong(bytes, offset + 4 + i * 8);
        }
        return array;
    }



    public static byte[] serialize(boolean value) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (value ? 1 : 0);
        return bytes;
    }



    public static boolean deserializeBoolean(byte[] bytes) {
        return bytes[0] == 1;
    }

    public static byte[] serializeArray(boolean[] array) {
        byte[] bytes = new byte[4 + array.length];
        return serializeArray(array, bytes, 0);
    }

    public static byte[] serializeArray(boolean[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        for (int i = 0; i < array.length; i++) {
            buffer[offset + 4 + i] = (byte) (array[i] ? 1 : 0);
        }
        return buffer;
    }

    public static boolean[] deserializeArrayBoolean(byte[] bytes) {
        return deserializeArrayBoolean(bytes, 0);
    }

    public static boolean[]  deserializeArrayBoolean(byte[] bytes, int offset) {
        int length = (bytes[offset] & 0xFF) << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8 | (bytes[offset + 3] & 0xFF);
        boolean[] array = new boolean[length];
        for (int i = 0; i < length; i++) {
            array[i] = bytes[offset + 4 + i] == 1;
        }
        return array;
    }

    public static byte[] serialize(byte value) {
        byte[] bytes = new byte[1];
        bytes[0] = value;
        return bytes;
    }

    public static byte deserializeByte(byte[] bytes) {
        return bytes[0];
    }


    public static byte[] serialize(char value, byte[] buffer, int offset) {
        buffer[offset] = (byte) (value >> 8);
        buffer[offset + 1] = (byte) (value);
        return buffer;
    }

    public static byte[] serialize(char value) {
        byte[] bytes = new byte[2];
        serialize(value, bytes, 0);
        return bytes;
    }

    public static char deserializeChar(byte[] bytes, int offset) {
        return (char) ((bytes[offset] & 0xFF) << 8 | (bytes[offset + 1] & 0xFF));
    }

    public static char deserializeChar(byte[] bytes) {
        return deserializeChar(bytes, 0);
    }

    public static byte[] serializeArray(char[] array) {
        byte[] bytes = new byte[4 + array.length * 2];
        return serializeArray(array, bytes, 0);
    }

    public static byte[] serializeArray(char[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        for (int i = 0; i < array.length; i++) {
            char value = array[i];
            buffer[offset + 4 + i * 2] = (byte) (value >> 8);
            buffer[offset + 5 + i * 2] = (byte) (value);
        }
        return buffer;
    }



    public static char[] deserializeArrayChar(byte[] buffer, int offset) {
        int length = (buffer[offset] & 0xFF) << 24 | (buffer[offset + 1] & 0xFF) << 16 | (buffer[offset + 2] & 0xFF) << 8 | (buffer[offset + 3] & 0xFF);
        char[] array = new char[length];
        for (int i = 0; i < length; i++) {
            array[i] = (char) ((buffer[offset + 4 + i * 2] & 0xFF) << 8 | (buffer[offset + 5 + i * 2] & 0xFF));
        }
        return array;
    }

    public static char[] deserializeArrayChar(byte[] buffer) {
        return deserializeArrayChar(buffer, 0);
    }

    public static byte[] serialize(short value) {
        byte[] bytes = new byte[2];
        serialize(value, bytes, 0);
        return bytes;
    }

    public static byte[] serialize(short value, byte[] buffer, int offset) {
        buffer[offset] = (byte) (value >> 8);
        buffer[offset + 1] = (byte) (value);
        return buffer;
    }


    public static short deserializeShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset] & 0xFF) << 8 | (bytes[offset + 1] & 0xFF));
    }

    public static short deserializeShort(byte[] bytes) {
        return deserializeShort(bytes, 0);
    }


    public static byte[] serializeArray(short[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        for (int i = 0; i < array.length; i++) {
            short value = array[i];
            buffer[offset + 4 + i * 2] = (byte) (value >> 8);
            buffer[offset + 5 + i * 2] = (byte) (value);
        }
        return buffer;
    }

    public static byte[] serializeArray(short[] array) {
        byte[] bytes = new byte[4 + array.length * 2];
        return serializeArray(array, bytes, 0);
    }

    public static short[] deserializeArrayShort(byte[] array, int offset) {
        int length = (array[offset] & 0xFF) << 24 | (array[offset + 1] & 0xFF) << 16 | (array[offset + 2] & 0xFF) << 8 | (array[offset + 3] & 0xFF);
        short[] result = new short[length];
        for (int i = 0; i < length; i++) {
            result[i] = (short) ((array[offset + 4 + i * 2] & 0xFF) << 8 | (array[offset + 5 + i * 2] & 0xFF));
        }
        return result;
    }

    public static byte[] serialize(float value) {
        return serialize(Float.floatToIntBits(value));
    }

    public static byte[] serialize(float value, byte[] buffer, int offset) {
        return serialize(Float.floatToIntBits(value), buffer, offset);
    }

    public static float deserializeFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(deserializeInt(bytes, offset));
    }

    public static float deserializeFloat(byte[] bytes) {
        return deserializeFloat(bytes, 0);
    }


    public static byte[] serializeArray(float[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        for (int i = 0; i < array.length; i++) {
            float value = array[i];
            serialize(Float.floatToIntBits(value), buffer, offset + 4 + i * 4);
        }
        return buffer;
    }



    public static byte[] serializeArray(float[] array) {
        byte[] bytes = new byte[4 + array.length * 4];
        return serializeArray(array, bytes, 0);
    }

    public static float[] deserializeArrayFloat(byte[] buffer, int offset) {
        int length = (buffer[offset] & 0xFF) << 24 | (buffer[offset + 1] & 0xFF) << 16 | (buffer[offset + 2] & 0xFF) << 8 | (buffer[offset + 3] & 0xFF);
        float[] array = new float[length];
        for (int i = 0; i < length; i++) {
            array[i] = Float.intBitsToFloat(deserializeInt(buffer, offset + 4 + i * 4));
        }
        return array;
    }

    public static float[] deserializeArrayFloat(byte[] buffer) {
        return deserializeArrayFloat(buffer, 0);
    }


    public static byte[] serialize(double value, byte[] buffer, int offset) {
        return serialize(Double.doubleToLongBits(value), buffer, offset);
    }

    public static byte[] serialize(double value) {
        return serialize(Double.doubleToLongBits(value));
    }

    public static double deserializeDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(deserializeLong(bytes, offset));
    }

    public static double deserializeDouble(byte[] bytes) {
        return deserializeDouble(bytes, 0);
    }

    public static byte[] serializeArray(double[] doubles, byte[] buffer, int offset) {
        buffer[offset] = (byte) (doubles.length >> 24);
        buffer[offset + 1] = (byte) (doubles.length >> 16);
        buffer[offset + 2] = (byte) (doubles.length >> 8);
        buffer[offset + 3] = (byte) (doubles.length);
        for (int i = 0; i < doubles.length; i++) {
            double value = doubles[i];
            serialize(Double.doubleToLongBits(value), buffer, offset + 4 + i * 8);
        }
        return buffer;
    }

    public static byte[] serializeArray(double[] doubles) {
        byte[] bytes = new byte[4 + doubles.length * 8];
        return serializeArray(doubles, bytes, 0);
    }

    public static double[] deserializeArrayDouble(byte[] buffer, int offset) {
        int length = (buffer[offset] & 0xFF) << 24 | (buffer[offset + 1] & 0xFF) << 16 | (buffer[offset + 2] & 0xFF) << 8 | (buffer[offset + 3] & 0xFF);
        double[] array = new double[length];
        for (int i = 0; i < length; i++) {
            array[i] = Double.longBitsToDouble(deserializeLong(buffer, offset + 4 + i * 8));
        }
        return array;
    }

    public static double[] deserializeArrayDouble(byte[] buffer) {
        return deserializeArrayDouble(buffer, 0);
    }

    public static byte[] serialize(String value, byte[] buffer, int offset) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer[offset] = (byte) (bytes.length >> 24);
        buffer[offset + 1] = (byte) (bytes.length >> 16);
        buffer[offset + 2] = (byte) (bytes.length >> 8);
        buffer[offset + 3] = (byte) (bytes.length);
        System.arraycopy(bytes, 0, buffer, offset + 4, bytes.length);
        return buffer;
    }



    public static byte[] serialize(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] buffer = new byte[4 + bytes.length];
        buffer[0] = (byte) (bytes.length >> 24);
        buffer[1] = (byte) (bytes.length >> 16);
        buffer[2] = (byte) (bytes.length >> 8);
        buffer[3] = (byte) (bytes.length);
        System.arraycopy(bytes, 0, buffer, 4, bytes.length);
        return buffer;

    }

    public static String deserializeString(byte[] bytes, int offset) {
        int length = (bytes[offset] & 0xFF) << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8 | (bytes[offset + 3] & 0xFF);
        return new String(bytes, offset + 4, length, StandardCharsets.UTF_8);
    }

    public static String deserializeString(byte[] bytes) {
        int stringBufferSize = deserializeInt(bytes);
        return new String(bytes, 4, stringBufferSize, StandardCharsets.UTF_8);
    }


    public static byte[] serializeArray(String[] array, byte[] buffer, int offset) {
        buffer[offset] = (byte) (array.length >> 24);
        buffer[offset + 1] = (byte) (array.length >> 16);
        buffer[offset + 2] = (byte) (array.length >> 8);
        buffer[offset + 3] = (byte) (array.length);
        int currentOffset = offset + 4;
        for (int i = 0; i < array.length; i++) {
            String value = array[i];
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer[currentOffset] = (byte) (bytes.length >> 24);
            buffer[currentOffset + 1] = (byte) (bytes.length >> 16);
            buffer[currentOffset + 2] = (byte) (bytes.length >> 8);
            buffer[currentOffset + 3] = (byte) (bytes.length);
            System.arraycopy(bytes, 0, buffer, currentOffset + 4, bytes.length);
            currentOffset += 4 + bytes.length;
        }
        return buffer;
    }


    public static byte[] serializeArray(String[] array) {
        byte[] bytes = new byte[4];
        int length = array.length;
        bytes[0] = (byte) (length >> 24);
        bytes[1] = (byte) (length >> 16);
        bytes[2] = (byte) (length >> 8);
        bytes[3] = (byte) (length);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(length * 16);
        byteArrayOutputStream.write(bytes, 0, 4);
        for (int i = 0; i < length; i++) {
            try {
                byteArrayOutputStream.write(serialize(array[i]));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static String[] deserializeArrayString(byte[] bytes) {
        return deserializeArrayString(bytes, 0);
    }


    public static String[] deserializeArrayString(byte[] bytes, int offset) {
        int length = (bytes[offset] & 0xFF) << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8 | (bytes[offset + 3] & 0xFF);
        String[] strings = new String[length];
        int currentOffset = offset + 4;
        for (int i = 0; i < length; i++) {
            int stringBufferSize = deserializeInt(bytes, currentOffset);
            strings[i] = new String(bytes, currentOffset + 4, stringBufferSize, StandardCharsets.UTF_8);
            currentOffset += 4 + stringBufferSize;
        }
        return strings;
    }





}
