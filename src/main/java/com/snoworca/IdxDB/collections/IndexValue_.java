package com.snoworca.IdxDB.collections;

public class IndexValue_ {

    private StoredInfo storedInfo;
    private Object value;
    private Object[] values;

    protected void clearIndex() {
        value = null;
        values = null;
    }


    public static IndexValue_ create(Object[] values, StoredInfo storedInfo) {
        IndexValue_ indexValue = new IndexValue_();
        indexValue.values = values;
        indexValue.storedInfo = storedInfo;
        return indexValue;
    }

    public static IndexValue_ create(Object value, StoredInfo storedInfo) {
        IndexValue_ indexValue = new IndexValue_();
        indexValue.value = value;
        indexValue.storedInfo = storedInfo;
        return indexValue;
    }




    public Object getValue() {
        return null;

    }



    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj == this) return true;

        Object indexValue = getValue();
        if(indexValue == null) return false;
        if(obj instanceof IndexValue_) {
            return indexValue.equals(((IndexValue_)obj).getValue());
        }
        else if(obj instanceof com.snoworca.IdxDB.collection.CSONItem) {
            return indexValue.equals(((com.snoworca.IdxDB.collection.CSONItem)obj).getIndexValue());
        }
        return obj.equals(indexValue);
    }



}
