package com.snoworca.IdxDB.collections;


public interface Restorable {
    public void restore(StoredInfo storedInfo);
    public void end();
}

