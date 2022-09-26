package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.exception.MissingIndexValueException;
import com.snoworca.cson.CSONObject;

import java.math.BigDecimal;

class CSONItem implements Comparable<CSONItem> {



    CSONItem(StoreDelegator storeDelegator, CSONObject csonObject, String key, int sort) {
        this.storeDelegator = storeDelegator;
        this.indexKey = key;
        this.sort = sort;
        this.csonObject = csonObject;
        /*if(this.csonObject == null && storeDelegator != null) {

        }*/
        this.indexValue = csonObject.opt(indexKey);
        if(this.indexValue == null) {
            throw new MissingIndexValueException(indexKey, csonObject);
        }
    }

    CSONItem(StoreDelegator storeDelegator, CSONObject csonObject, String key,Object indexValue) {
        this.storeDelegator = storeDelegator;
        this.indexKey = key;
        this.sort = 1;
        this.csonObject = csonObject;
        /*if(this.csonObject == null && storeDelegator != null) {
            isFileCache = true;
        }*/
        this.indexValue = indexValue;
    }


    CSONItem(StoreDelegator storeDelegator, String key,Object indexValue, int sort) {
        this.storeDelegator = storeDelegator;
        this.indexKey = key;
        this.sort = sort;
        this.csonObject = csonObject;
        if(this.csonObject == null && storeDelegator != null) {
            isStorageSaved = true;
        }
        this.indexValue = indexValue;
    }


    private volatile CSONObject csonObject;
    private String indexKey = null;
    private Object indexValue;
    private int sort = 0;
    private boolean isStorageSaved = false;
    private long storagePos = -1;
    private StoreDelegator storeDelegator;
    private boolean isChanged = false;


    public boolean isChanged() {
        return isChanged;
    }

    public CSONObject getCsonObject() {
        if(csonObject == null) {
            byte[] buffer = storeDelegator.load(storagePos);
            CSONObject loadJSON = new CSONObject(buffer);
            if(!isStorageSaved) csonObject = loadJSON;
            else return loadJSON;
        }
        return csonObject;
    }

    public void setCsonObject(CSONObject jsonObject) {
        this.csonObject = jsonObject;
        this.isChanged = true;
    }

    public long getStoragePos() {
        return storagePos;
    }

    public void storeIfNeed() {
        if(storagePos > -1) return;
        storagePos = storeDelegator.cache(csonObject.toByteArray());
        isStorageSaved = true;
    }

    public void release() {
        storagePos = -1;
        csonObject = null;
        storeDelegator = null;
    }



    public void setStore(boolean enable) {
        if(enable) {
            if(this.csonObject != null && (!isStorageSaved || isChanged)) {
                storagePos = storeDelegator.cache(csonObject.toByteArray());
            }
            this.csonObject = null;
        }
        else if(this.csonObject == null) {
            this.csonObject = getCsonObject();
        }
        isStorageSaved = enable;
    }

    protected void setStoragePos(long pos) {
        this.storagePos = pos;
        if(pos > -1 && storeDelegator != null) {
            isStorageSaved = true;
        }
    }


    @Override
    public int compareTo(CSONItem o) {
        Object targetObj = o.getIndexValue();
        Object thisObj = indexValue;
        if(targetObj == null && thisObj != null) {
            return sort;
        } else if(targetObj != null && thisObj == null) {
            return -sort;
        } else if(targetObj == null && thisObj == null) {
            return 0;
        }

        if(targetObj instanceof String && thisObj instanceof String) {
            return ((String)thisObj).compareTo((String)targetObj) * sort;
        }
        if(targetObj instanceof Number && thisObj instanceof  Number) {
            return compareTo((Number)thisObj, (Number)targetObj) * sort;
        }
        if(targetObj instanceof Boolean && thisObj instanceof  Boolean) {
            return thisObj.equals(targetObj) ? 0 :
                   thisObj.equals(Boolean.TRUE) && thisObj.equals(Boolean.FALSE) ? sort : -1 * sort;
        }
        return 0;
    }

    public Object getIndexValue() {
        return this.indexValue;
    }

    public void setIndexValue(Object object) {
        this.indexValue = object == null ? Integer.valueOf(0) : object;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) return true;
        else if(obj == null) return false;
        if(obj instanceof CSONItem && /*compareTo((CSONItem)obj) == 0*/ indexValue.equals(((CSONItem)obj).indexValue)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        Object obj = indexValue;
        if(obj == null) return 0;
        return obj.hashCode();
    }





    @Override
    public String toString() {
        return csonObject == null ? "" : csonObject.toString();
    }

    private static int compareTo(Number n1, Number n2) {
        // ignoring null handling
        BigDecimal b1 = BigDecimal.valueOf(n1.doubleValue());
        BigDecimal b2 = BigDecimal.valueOf(n2.doubleValue());
        return b1.compareTo(b2);
    }


}
