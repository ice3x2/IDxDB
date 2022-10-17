package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.IdxDB;
import com.snoworca.IdxDB.OP;
import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexLinkedMapTest {

    @Test
    public void indexMapCacheTest() throws IOException {
        File dbFile = new File("indexMapFindTest.db");
        dbFile.delete();
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collection = idxDB.newIndexMapBuilder("map").memCacheSize(100).index("index", -1).setAccessOrder(true).create();
        for(int i = 0; i < 1000; ++i) {
            collection.add(new CSONObject().put("index", i ).put("data", i));
        }
        collection.commit();
        assertEquals(1000, collection.size());
        for(int i = 999; i >= 0; --i) {
            collection.remove(new CSONObject().put("index", i ).put("data", i));
        }
        collection.commit();
        assertEquals(0, collection.size());
    }

    @Test
    public void indexMapFindAscTest() throws IOException {
        File dbFile = new File("indexMapFindTest.db");
        dbFile.delete();
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collection = idxDB.newIndexMapBuilder("map").memCacheSize(100).index("index", -1).setAccessOrder(true).create();
        for(int i = 0; i < 1000; ++i) {
            collection.add(new CSONObject().put("index", i ).put("data", i));
        }
        collection.commit();
        List<CSONObject> result = collection.findByIndex(100, FindOption.fromOP(OP.lt),100);
        assertEquals(100,result.size());
        assertEquals(0,result.get(99).optInt("index"));
        assertEquals(99,result.get(0).optInt("index"));

        result = collection.findByIndex(800, FindOption.fromOP(OP.gte),800);
        assertEquals(200,result.size());
        assertEquals(800,result.get(199).optInt("index"));
        assertEquals(999,result.get(0).optInt("index"));
    }

    @Test
    public void indexMapFindDescTest() throws IOException {
        File dbFile = new File("indexMapFindTest.db");
        //dbFile.delete();
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collection = idxDB.newIndexMapBuilder("map").memCacheSize(100).index("index", 1).setAccessOrder(true).create();
        for(int i = 0; i < 1000; ++i) {
            collection.add(new CSONObject().put("index", i ).put("data", i));
        }
        collection.commit();
        List<CSONObject> result = collection.findByIndex(100, FindOption.fromOP(OP.lt),100);
        assertEquals(100,result.size());
        assertEquals(0,result.get(0).optInt("index"));
        assertEquals(99,result.get(99).optInt("index"));


    }

}