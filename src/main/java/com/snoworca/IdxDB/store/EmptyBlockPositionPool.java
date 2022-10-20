package com.snoworca.IdxDB.store;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;

public class EmptyBlockPositionPool {

    private final TreeMap<Integer, ArrayDeque<EmptyBlockInfo>> emptyBlockPositionMap = new TreeMap<>();
    private final float limitRatio;
    private final boolean isLimitRatio;


    public EmptyBlockPositionPool() {
        this(0);
    }

    public EmptyBlockPositionPool(float limitRatio) {
        limitRatio = limitRatio < 0 ? 0 : limitRatio;
        this.isLimitRatio = !(limitRatio < 0.01f);
        this.limitRatio = limitRatio + 1.0f;
    }

    public void offer(long position, int capacity) {
        EmptyBlockInfo emptyBlockInfo = new EmptyBlockInfo(position, capacity);
        offer(emptyBlockInfo);
    }

    public void offer(EmptyBlockInfo emptyBlockInfo) {
        int capacity = emptyBlockInfo.capacity;
        ArrayDeque<EmptyBlockInfo> emptyBlockPositionList = emptyBlockPositionMap.get(capacity);
        if (emptyBlockPositionList == null) {
            emptyBlockPositionList = new ArrayDeque<>();
            emptyBlockPositionMap.put(capacity, emptyBlockPositionList);
        }
        emptyBlockPositionList.offer(emptyBlockInfo);
    }

    public boolean isEmpty() {
        return emptyBlockPositionMap.isEmpty();
    }

    public EmptyBlockInfo obtain(int capacity) {
        if(emptyBlockPositionMap.isEmpty()) {
            return null;
        }
        Map.Entry<Integer, ArrayDeque<EmptyBlockInfo>> entry = emptyBlockPositionMap.ceilingEntry(capacity);
        if(entry == null) {
            return null;
        }
        int emptyBlockCapacity =  entry.getKey();
        if(emptyBlockCapacity < capacity || (isLimitRatio && emptyBlockCapacity > capacity * limitRatio)) {
            return null;
        }
        ArrayDeque<EmptyBlockInfo> emptyBlockPositionList = entry.getValue();
        EmptyBlockInfo emptyBlockInfo = emptyBlockPositionList.pop();
        if(emptyBlockPositionList.isEmpty()) {
            emptyBlockPositionMap.remove(entry.getKey());
        }
        return emptyBlockInfo;
    }

    public static class EmptyBlockInfo {
        private long pos;
        private int capacity;

        EmptyBlockInfo(long pos, int capacity) {
            this.pos = pos;
            this.capacity = capacity;
        }

        public long getPosition() {
            return pos;
        }

        public int getCapacity() {
            return capacity;
        }
    }

}
