package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

public interface StoreDelegator {

        public long storeData(long pos,Object index, CSONObject csonObject);
        public CSONArray loadIndex(long pos);

        public CSONObject loadData(long pos);


}
