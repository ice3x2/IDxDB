package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.IdxDB;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class IndexMapTest {

    @Test
    public void indexMapFindTest() throws IOException {
        File dbFile = new File("indexMapFindTest.db");
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        idxDB.newIndexMapBuilder("map")

    }

}