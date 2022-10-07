package com.snoworca.IdxDB.collection;

public class IndexValueCache extends IndexValue {

    private Object indexValue;
    private Integer hashCode = null;

    IndexValueCache(Object indexValue) {
        this.indexValue = indexValue;
    }

    @Override
    Object getIndexValue() {
        return indexValue;
    }

    @Override
    public int hashCode() {
        if(hashCode == null) {
            hashCode = indexValue.hashCode();
        }
        return hashCode;
    }

    @Override
    void changeIndexValue(CSONItem csonItem) {
        indexValue = csonItem.getIndexValue();
        hashCode = csonItem.hashCode();
    }
}
