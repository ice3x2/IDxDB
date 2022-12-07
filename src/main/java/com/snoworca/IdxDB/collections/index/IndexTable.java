package com.snoworca.IdxDB.collections.index;

import com.snoworca.IdxDB.CompareUtil;
import com.snoworca.IdxDB.OP;
import com.snoworca.IdxDB.collection.CSONItem;
import com.snoworca.IdxDB.collections.IndexCollection;
import com.snoworca.IdxDB.collections.IndexValue_;
import com.snoworca.IdxDB.collections.StoredInfo;
import com.snoworca.IdxDB.util.CusLinkedHashMap;
import com.snoworca.cson.CSONElement;
import com.snoworca.cson.CSONObject;

public class IndexTable {
    private String name;
    private IndexInfo[] indexInfos;

    private CusLinkedHashMap<IndexValue, StoredInfo> indexHashMap;
    private IndexCollection<IndexValue> indexValueRows;
    private IndexCollection<IndexValue> cacheRows;
    private IndexTableOption option;
    private boolean isUnique;

    private void init() {
        if (isHashMap()) {
            indexHashMap = new CusLinkedHashMap<>(option.getMemCacheSize());
        }

        indexValueRows = new IndexCollection<>();
    }

    private boolean isHashMap() {
        if(option.isHashMap()) return true;
        for(IndexInfo indexInfo : indexInfos) {
            if(indexInfo.sort != 0) return false;
        }
        indexInfos = option.getIndexItems();
        option.setHashMap(true);
        return true;
    }



    public void remove(Object indexKey) {

    }

    private IndexValue makeIndexValue(CSONObject csonObject) {
        IndexValue indexValue = null;
        if(indexInfos.length == 1) {
            Object value = csonObject.opt(indexInfos[0].indexKey);
            if(value == null || value instanceof CSONElement) {
                value = EmptyIndexValue.INSTANCE;
            }
            indexValue = IndexValue.create(value);
        }
        else {
            Object[] indexValues = new Object[indexInfos.length];
            for (int i = 0; i < indexInfos.length; i++) {
                indexValues[i] = csonObject.opt(indexInfos[i].indexKey);
                if (indexValues[i] == null || indexValues[i] instanceof CSONElement) {
                    indexValues[i] = EmptyIndexValue.INSTANCE;
                }
            }
            indexValue = IndexValue.create(indexValues);
        }
        return indexValue;
    }


    public void add(CSONObject csonObject, StoredInfo storedInfo) {
        IndexValue indexValue = makeIndexValue(csonObject);
        IndexValue existIndexValue = indexValueRows.get(indexValue);
        if(existIndexValue != null) {
            if(isUnique) {
                throw new RuntimeException("Unique index violation");
            }
            else {
                existIndexValue.addStoredInfo(storedInfo);
            }
        }
        else {
            indexValue.addStoredInfo(storedInfo);
            indexValueRows.add(indexValue);
        }
        if(!option.isMemCacheIndex()) {
            indexValue.clearIndex();
        }
    }





    public String getName() {
        return name;
    }


}
