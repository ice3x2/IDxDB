package com.snoworca.IdxDB.collections.index;

import com.snoworca.IdxDB.collections.StoredInfo;

import java.util.ArrayList;

public class IndexValue implements  Comparable<IndexValue> {

    private ArrayList<StoredInfo> storedInfoList = new ArrayList<>();
    private Object value;
    private Object[] values;


    protected void clearIndex() {
        value = null;
        values = null;
    }


    public static IndexValue create(Object[] values) {
        IndexValue indexValue = new IndexValue();
        indexValue.values = values;
        return indexValue;
    }

    public static IndexValue create(Object value) {
        IndexValue indexValue = new IndexValue();
        indexValue.value = value;
        return indexValue;
    }




    public Object getValue() {
        return null;
    }

    public void addStoredInfo(StoredInfo storedInfo) {
        storedInfoList.add(storedInfo);
    }



    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj == this) return true;

        Object indexValue = getValue();
        if(indexValue == null) return false;
        if(obj instanceof IndexValue) {
            return indexValue.equals(((IndexValue)obj).getValue());
        }
        else if(obj instanceof com.snoworca.IdxDB.collection.CSONItem) {
            return indexValue.equals(((com.snoworca.IdxDB.collection.CSONItem)obj).getIndexValue());
        }
        return obj.equals(indexValue);
    }





    @Override
    public int compareTo(IndexValue o) {
        return 0;
    }
}
