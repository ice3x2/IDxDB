package com.snoworca.IDxDB;

import com.snoworca.IDxDB.serialization.Column;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

class IDxDBTest {

    @Test
    public void defaultTest() {
        //IDxDB db = IDxDBBuilder.open("./defaultTest").build();

        IDxDB db = new IDxDB();
        XCollection<TestObject> testObjectXList = db.getOrCreateList("table");
        testObjectXList.init();


    }


    public static class TestObject {
        @Column
        String uuid;

        @Column
        ArrayList<Long> values;
    }

}