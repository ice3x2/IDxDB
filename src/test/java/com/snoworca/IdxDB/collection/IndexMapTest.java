package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.IdxDB;
import com.snoworca.IdxDB.OP;
import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Find;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexMapTest {

    @Test
    public void indexMapFindTest() throws IOException {
        File dbFile = new File("indexMapFindTest.db");
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collection = idxDB.newIndexMapBuilder("map").memCacheSize(100).index("index", 1).setAccessOrder(true).create();
        for(int i = 0; i < 1000; ++i) {
            collection.add(new CSONObject().put("index", i + 1 ).put("data", i));
        }
        List<CSONObject> result = collection.findByIndex("100", FindOption.fromOP(OP.lt),1000);
        assertEquals(100,result.size());


    }

}