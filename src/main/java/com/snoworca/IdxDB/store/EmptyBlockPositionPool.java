package com.snoworca.IdxDB.store;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;

public class EmptyBlockPositionPool {

    private final TreeMap<Integer, ArrayDeque<DataBlock>> emptyBlockPositionMap = new TreeMap<>();
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




    public DataBlock push(DataBlock dataBlock) {
        int capacity = dataBlock.getCapacity();

        ArrayDeque<DataBlock> emptyBlockPositionList = emptyBlockPositionMap.get(capacity);
        if (emptyBlockPositionList == null) {
            emptyBlockPositionList = new ArrayDeque<>();
            emptyBlockPositionMap.put(capacity, emptyBlockPositionList);
        }
        emptyBlockPositionList.offer(dataBlock);
        return dataBlock;
    }

    public boolean isEmpty() {
        return emptyBlockPositionMap.isEmpty();
    }

    public DataBlock obtain(int capacity) {
        if(emptyBlockPositionMap.isEmpty()) {
            return null;
        }
        Map.Entry<Integer, ArrayDeque<DataBlock>> entry = emptyBlockPositionMap.ceilingEntry(capacity);
        if(entry == null) {
            return null;
        }
        int emptyBlockCapacity =  entry.getKey();
        if(emptyBlockCapacity < capacity || (isLimitRatio && emptyBlockCapacity > capacity * limitRatio)) {
            return null;
        }
        ArrayDeque<DataBlock> emptyBlockPositionList = entry.getValue();
        DataBlock dataBlock = emptyBlockPositionList.pop();
        if(emptyBlockPositionList.isEmpty()) {
            emptyBlockPositionMap.remove(entry.getKey());
        }
        return dataBlock;
    }

}
