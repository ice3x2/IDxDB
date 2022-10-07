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
    void changeIndexValue(CSONItem csonItem) {
        this.csonItem = csonItem;
    }
}
