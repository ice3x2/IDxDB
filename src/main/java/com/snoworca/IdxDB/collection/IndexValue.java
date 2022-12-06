package com.snoworca.IdxDB.collection;

public class IndexValue {

    public Object getIndexValue() {
        return null;

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



}
