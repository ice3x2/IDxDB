package com.snoworca.IdxDB.collection;

public abstract class IndexValue {

    abstract Object getIndexValue();

    public static IndexValue newIndexValueCache(Object indexValue) {
        return new IndexValueCache(indexValue);
    }

    public static IndexValue newIndexValueItem(CSONItem csonItem) {
        return new IndexValueItem(csonItem);
    }



    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if(obj == this) return true;

        Object indexValue = getIndexValue();
        if(indexValue == null) return false;
        if(obj instanceof IndexValue) {
            return indexValue.equals(((IndexValue)obj).getIndexValue());
        }
        else if(obj instanceof CSONItem) {
            return indexValue.equals(((CSONItem)obj).getIndexValue());
        }
        return obj.equals(indexValue);
    }


    abstract void changeIndexValue(CSONItem csonItem);

}
