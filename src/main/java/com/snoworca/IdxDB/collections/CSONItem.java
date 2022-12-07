package com.snoworca.IdxDB.collections;

import com.snoworca.IdxDB.collection.StoreDelegator;
import com.snoworca.IdxDB.collection.StoredInfo;
import com.snoworca.IdxDB.exception.MissingIndexValueException;
import com.snoworca.cson.CSONObject;

import java.math.BigDecimal;
import java.util.regex.Pattern;

class CSONItem implements Comparable<CSONItem> {

    private final static Pattern NUM_PATTERN = Pattern.compile("[+-]?([0-9]*[.])?[0-9]+");


    static CSONItem createIndexItem(Object indexValue, int sort) {
        CSONItem csonItem = new CSONItem();
        csonItem.indexValue = indexValue;
        csonItem.sort = sort;
        return csonItem;

    }


    private CSONItem() {}

    CSONItem(StoreDelegator storeDelegator, CSONObject csonObject, String IndexKey, int sort, boolean memCacheIndex) {
        this.storeDelegator = storeDelegator;
        this.indexKey = IndexKey;
        this.sort = sort;
        this.csonObject = csonObject;
        this.indexValue = csonObject.opt(indexKey);
        this.isMemCacheIndex = memCacheIndex;
        if(this.indexValue == null) {
            throw new MissingIndexValueException(indexKey, csonObject);
        }
    }

    CSONItem(StoreDelegator storeDelegator, String indexValue, String IndexKey, int sort, boolean memCacheIndex) {
        this.storeDelegator = storeDelegator;
        this.indexKey = IndexKey;
        this.sort = sort;
        this.indexValue = indexValue;
        this.isMemCacheIndex = memCacheIndex;
        if(this.indexValue == null) {
            throw new MissingIndexValueException(indexKey, new CSONObject());
        }
    }


    private volatile CSONObject csonObject;
    private String indexKey = null;
    private Object indexValue;
    private int sort = 0;
    private boolean isStorageSaved = false;
    private long dataPos = -1;
    private int storeCapacity = -1;
    private StoreDelegator storeDelegator;
    private boolean isChanged = false;

    private boolean isMemCacheIndex = true;

    public boolean isChanged() {
        return isChanged;
    }

    public CSONObject getCsonObject() {
        if(csonObject == null) {
            StoredInfo info = storeDelegator.loadData(dataPos);
            CSONObject csonObject = info.getCsonObject();
            storeCapacity = info.getCapacity();
            dataPos = info.getPosition();
            return csonObject;
        }
        return csonObject;
    }

    public void setStoreCapacity(int storeCapacity) {
        this.storeCapacity = storeCapacity;
    }

    public int getStoreCapacity() {
        return storeCapacity;
    }



    public void setCsonObject(CSONObject jsonObject) {
        this.csonObject = jsonObject;
        this.isChanged = true;
    }

    /*public long getStoragePos() {
        return storagePos;
    }*/

    public void release() {
        dataPos = -1;
        csonObject = null;
        storeDelegator = null;
        indexValue = null;
        indexKey = null;
        storeCapacity = -1;
    }



    public void clearCache() {
        if(!isMemCacheIndex)  {
            this.indexValue = null;
        }
        this.csonObject = null;
    }

    public void clearIndexCache() {
        if(!isMemCacheIndex) this.indexValue = null;
    }

    public void cache() {
        if(this.csonObject == null) {
            this.csonObject = getCsonObject();
            if(this.indexValue == null) {
                this.indexValue = this.csonObject.opt(indexKey);
            }
        }
    }


    public void setStore_(boolean enable) {
        if(enable) {
            if(this.csonObject != null && (!isStorageSaved || isChanged)) {
                //store();
            }
            if(!isMemCacheIndex) this.indexValue = null;
            this.csonObject = null;
        }
        else if(this.csonObject == null) {
            this.csonObject = getCsonObject();
            if(this.indexValue == null) {
                this.indexValue = this.csonObject.opt(indexKey);
            }
        }
    }


    public void replace(CSONObject jsonObject) {
        setCsonObject(jsonObject);
    }



    protected boolean isStored() {
        return this.dataPos > -1;
    }



    protected void setStoragePos_(long pos) {
        this.dataPos = pos;
        if(pos > -1 && storeDelegator != null) {
            isStorageSaved = true;
        }
    }



    @Override
    public int compareTo(CSONItem o) {
        Object targetObj = o.getIndexValue();
        return compareIndex(targetObj);
    }


    public int compareIndex(Object targetIndexValue) {
        Object thisObj = getIndexValue();
        if(targetIndexValue == null && thisObj != null) {
            return sort;
        } else if(targetIndexValue != null && thisObj == null) {
            return -sort;
        } else if(targetIndexValue == null && thisObj == null) {
            return 0;
        }

        if(targetIndexValue instanceof String && thisObj instanceof String) {
            return ((String)thisObj).compareTo((String)targetIndexValue) * sort;
        }
        if(targetIndexValue instanceof Number && thisObj instanceof  Number) {
            return compareTo((Number)thisObj, (Number)targetIndexValue) * sort;
        }
        if(targetIndexValue instanceof Boolean && thisObj instanceof  Boolean) {
            return thisObj.equals(targetIndexValue) ? 0 :
                    thisObj.equals(Boolean.TRUE) && thisObj.equals(Boolean.FALSE) ? sort : -1 * sort;
        }

        if(targetIndexValue instanceof String && thisObj instanceof  Number) {
            try {
                Double targetDoubleValue = Double.valueOf((String)targetIndexValue);
                return compareTo((Number)thisObj, targetDoubleValue) * sort;
            } catch (NumberFormatException ignored) {}
        }
        if(targetIndexValue instanceof Number && thisObj instanceof String) {
            try {
                Double thisDoubleValue = Double.valueOf((String)thisObj);
                return compareTo(thisDoubleValue, (Number)targetIndexValue) * sort;
            } catch (NumberFormatException ignored) {}
        }

        return thisObj.toString().compareTo(targetIndexValue.toString()) * sort;


    }


    public long getStoragePos() {
        return dataPos;
    }

    public Object getIndexValue() {
        if(isMemCacheIndex || this.indexValue != null) {
            return this.indexValue;
        }
        return getCsonObject().opt(indexKey);
    }

    /*public void setIndexValue(Object object) {
        this.indexValue = object == null ? Integer.valueOf(0) : object;
    }*/

    @Override
    public boolean equals(Object obj) {
        if(obj == this) return true;
        else if(obj == null) return false;
        Object indexValue = getIndexValue();

        if(obj instanceof CSONItem && /*compareTo((CSONItem)obj) == 0*/ indexValue.equals(((CSONItem)obj).getIndexValue())) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        Object obj = getIndexValue();
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
