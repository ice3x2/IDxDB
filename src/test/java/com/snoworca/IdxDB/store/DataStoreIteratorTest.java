package com.snoworca.IdxDB.store;

import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DataStoreIteratorTest {

    @Test
    public void test() throws IOException {
        File file = new File("test.dat");
        file.delete();
        DataStoreOptions options = new DataStoreOptions();
        options.setIterableBufferSize(1024 * 1024);
        DataStore dataStore = new DataStore(file,options);
        dataStore.open();

        int testCase = 412230;
        ArrayList<Long> removePos = new ArrayList<>();
        ArrayList<CSONObject> csonObjects = new ArrayList<>();
        for(int i = 0; i < testCase; ++i) {
            CSONObject csonObject = new CSONObject().put("key", i);
            DataBlock dataBlock = dataStore.write(10000, csonObject.toBytes());
            if(i %  1000 == 0) {
                removePos.add(dataBlock.getPosition());
            } else {
                csonObjects.add(csonObject);
            }
            if(i % 1000000 == 0) {

                System.out.println(i + "번 쓰기 완료");
            }
        }

        for(Long pos : removePos) {
            dataStore.unlink(pos);
        }

        System.out.println((file.length() / 1024 / 1024) + "MB");
        long current = System.currentTimeMillis();
        int count = 0;
        for(DataBlock dataBlock : dataStore) {
            ++count;
            //System.out.println(new CSONObject(dataBlock.getData()));
        }
        System.out.println("읽기 완료 : " + (System.currentTimeMillis() - current) + "ms");

        assertEquals(testCase - removePos.size(), count);
        assertEquals(dataStore.getEmptyBlockPositionPoolSize(), removePos.size());

        dataStore.close();
        dataStore = new DataStore(file,options);
        dataStore.open();

        current = System.currentTimeMillis();
        count = 0;
        for(DataBlock dataBlock : dataStore) {
            CSONObject csonObject = new CSONObject(dataBlock.getData());
            assertArrayEquals(csonObjects.get(count).toBytes(), csonObject.toBytes());
            ++count;
            //System.out.println(new CSONObject(dataBlock.getData()));
        }
        System.out.println("읽기 완료 : " + (System.currentTimeMillis() - current) + "ms");

        assertEquals(testCase - removePos.size(), count);
        assertEquals(dataStore.getEmptyBlockPositionPoolSize(), removePos.size());



    }

}