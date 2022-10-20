package com.snoworca.IdxDB.store;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;

public class EmptyBlockPositionPool {

    private final TreeMap<Integer, ArrayDeque<DataBlock>> emptyBlockPositionMap = new TreeMap<>();

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

    public DataBlock get(int capacity) {
        if(emptyBlockPositionMap.isEmpty()) {
            return null;
        }
        Map.Entry<Integer, ArrayDeque<DataBlock>> entry = emptyBlockPositionMap.floorEntry(capacity);
        if(entry == null || entry.getKey() < capacity) {
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
