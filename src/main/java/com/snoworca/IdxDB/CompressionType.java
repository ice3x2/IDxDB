package com.snoworca.IdxDB;

public enum CompressionType {
    NONE(0),
    GZIP(1),
    Deflater(2),
    SNAPPY(5);

    private final int value;

    CompressionType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static CompressionType fromValue(int value) {
        for (CompressionType type : CompressionType.values()) {
            if (type.getValue() == value) {
                return type;
            }
        }
        return null;
    }
}
