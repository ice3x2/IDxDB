package com.snoworca.IdxDB.collection;

import com.snoworca.cson.CSONObject;

public interface StoreDelegator {

        public long storeIndex(long dataPos,Object index);

        public long storeData(CSONObject csonObject);
        public CSONObject load(long pos);


}
