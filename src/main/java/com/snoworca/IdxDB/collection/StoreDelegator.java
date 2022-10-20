package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.store.DataBlock;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;

public interface StoreDelegator {

        public StoredInfo storeData(long pos, CSONObject csonObject);

        public StoredInfo loadData(long pos);





}
