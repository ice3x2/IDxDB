package com.snoworca.IdxDB.collection;

public class IndexValueItem extends IndexValue {

    private CSONItem csonItem;

    IndexValueItem(CSONItem csonItem) {
        this.csonItem = csonItem;
    }

    @Override
    Object getIndexValue() {
        return csonItem.getIndexValue();
    }

    @Override
    public int hashCode() {
        return csonItem.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof IndexValueItem)) return false;
        return obj == this || getIndexValue().equals(  ((IndexValueItem) obj).getIndexValue());
    }

    @Override
    void changeIndexValue(CSONItem csonItem) {
        this.csonItem = csonItem;
    }
}
