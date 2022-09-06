package com.snoworca.IDxDB;

public class IndexCommand {
    public final static byte TYPE_XLIST = 1;


    public final static byte CMD_NEW = 1;
    public final static byte CMD_ADD = 2;
    public final static byte CMD_REMOVE_BY_OBJECT = 3;
    public final static byte CMD_ADD_ALL = 4;
    public final static byte CMD_ADD_ALL_WITH_INDEX = 5;
    public final static byte CMD_REMOVE_ALL = 6;
    public final static byte CMD_RETAIN_ALL = 7;
    public final static byte CMD_CLEAR = 8;
    public final static byte CMD_GET = 9;
    public final static byte CMD_SET_WITH_INDEX = 10;
    public final static byte CMD_ADD_WITH_INDEX = 11;
    public final static byte CMD_REMOVE_BY_INDEX = 12;
    public final static byte CMD_REPLACE = 13;


    private byte type;
    private byte cmd;

    private Object argObject;
    private int argIndex;


    IndexCommand(byte type, byte cmd) {
        this.type = type;
        this.cmd = cmd;
    }


    IndexCommand(byte type, byte cmd, int index) {
        this.type = type;
        this.cmd = cmd;
        this.argIndex = index;
    }


    IndexCommand(byte type, byte cmd, Object argObject) {
        this.type = type;
        this.cmd = cmd;
        this.argObject = argObject;
    }

    IndexCommand(byte type, byte cmd,int index, Object argObject) {
        this.type = type;
        this.cmd = cmd;
        this.argIndex = index;
        this.argObject = argObject;
    }




}
