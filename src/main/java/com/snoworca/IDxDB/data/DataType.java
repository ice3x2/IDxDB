package com.snoworca.IDxDB.data;

public interface DataType {
    public final static byte TYPE_NULL = 0;
    public final static byte TYPE_BYTE = 1;
    public final static byte TYPE_SHORT = 2;
    public final static byte TYPE_CHAR = 3;
    public final static byte TYPE_INT = 4;
    public final static byte TYPE_FLOAT = 5;
    public final static byte TYPE_LONG = 6;
    public final static byte TYPE_DOUBLE = 7;
    public final static byte TYPE_BOOLEAN = 8;
    public final static byte TYPE_STRING = 9;
    public final static byte TYPE_BYTE_ARRAY = 10;
    public final static byte TYPE_OBJECT = 11;

    public static boolean isNumberType(byte type) {
        return type >= TYPE_BYTE && type <= TYPE_DOUBLE;
    }

    public static int getNumberTypeLength(byte type) {
        switch (type) {
            case TYPE_BYTE:
            case TYPE_BOOLEAN:
                return 1;
            case TYPE_SHORT:
            case TYPE_CHAR:
                return 2;
            case TYPE_INT:
            case TYPE_FLOAT:
                return 4;
            case TYPE_LONG:
            case TYPE_DOUBLE:
                return 8;
            default:
                return -1;
        }
    }

    public static boolean checkNumberTypeLength(byte type,int length) {
        int numLen = getNumberTypeLength(type);
        return numLen == -1 || length == numLen;

    }
}
