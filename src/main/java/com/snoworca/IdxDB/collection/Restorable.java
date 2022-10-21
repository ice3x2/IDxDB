package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.collection.StoredInfo;
import com.snoworca.cson.CSONObject;

public interface Restorable {
    public void restore(StoredInfo storedInfo);
    public void end();
}

