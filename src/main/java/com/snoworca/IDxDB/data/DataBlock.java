package com.snoworca.IDxDB.data;

public class DataBlock {
    private DataHeader header;
    private byte[] data;

    public static DataBlock newInstance(Number value) {
        DataBlock dataPayload = new DataBlock();

        return dataPayload;
    }

    public static DataBlock newInstance(Boolean value) {
        DataBlock dataPayload = new DataBlock();

        return dataPayload;
    }

    public static DataBlock newInstance(CharSequence value) {
        DataBlock dataPayload = new DataBlock();

        return dataPayload;
    }

    public static DataBlock newInstance() {
        DataBlock dataPayload = new DataBlock();

        return dataPayload;
    }



    public static DataBlock parseData(DataHeader header, byte[] buffer) {
        DataBlock dataBlock = new DataBlock();
        dataBlock.header = header;
        int len = header.getLength();
        byte dataType = dataBlock.header.getType();
        boolean isNumber = DataType.isNumberType(dataType);
        if(!DataType.checkNumberTypeLength(dataType, len)) {
            //TODO 예외처리 들어가야함.
        }
        if(isNumber) {

        }


        return dataBlock;
    }



}
