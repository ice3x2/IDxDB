package com.snoworca.IdxDB.collection;

import com.snoworca.IdxDB.IdxDB;
import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;


import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexMemCacheOptionTest {

    @Test
    public void indexSetMemCacheOptionTest() throws IOException {
        File dbFile = new File("indexMemCacheTest.db");
        dbFile.delete();
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collectionMemCache = idxDB. newIndexTreeSetBuilder("memCacheIndex").index("key", 1).memCacheSize(3000).setMemCacheIndex(true).create();
        IndexCollection collectionNoCache = idxDB. newIndexTreeSetBuilder("noCacheIndex").index("key", 1).memCacheSize(3000).setMemCacheIndex(false).create();

        for(int i = 0; i < 1000; ++i) {
            collectionMemCache.add(new CSONObject().put("key", i + "").put("value", i));
            collectionNoCache.add(new CSONObject().put("key", i + "").put("value", i));
        }
        collectionMemCache.commit();
        collectionNoCache.commit();
        System.out.println("초기화완료");

        long start = System.currentTimeMillis();
        for(int i = 50000; i < 100000; ++i) {
            collectionMemCache.add(new CSONObject().put("key", i + "").put("value", i));
            collectionMemCache.commit();
        }
        collectionMemCache.removeByIndex("1");
        collectionMemCache.removeByIndex("50000");
        collectionMemCache.removeByIndex("100000");
        collectionMemCache.commit();

        long timeLap1 = System.currentTimeMillis() - start + 1;
        System.out.println("인덱스 값 메모리 캐쉬:" + timeLap1 + "ms");

        start = System.currentTimeMillis();
        for(int i = 50000; i < 100000; ++i) {
            collectionNoCache.add(new CSONObject().put("key", i + "").put("value", i));
            collectionNoCache.commit();
        }
        collectionMemCache.removeByIndex("1");
        collectionMemCache.removeByIndex("50000");
        collectionMemCache.removeByIndex("100000");
        collectionMemCache.commit();
        long timeLap2 = System.currentTimeMillis() - start + 1;
        System.out.println("인덱스 값 노 캐쉬:"+ timeLap2 + "ms");


        System.out.println("DBFile size: " + (dbFile.length() / (1024 * 1024)) + "mb");

        System.out.println("인덱스 값 메모리 캐쉬:" + timeLap1 + "ms");

        assertTrue(timeLap2 > ( timeLap1 * 1.1));
        dbFile.delete();

        idxDB = IdxDB.newMaker(dbFile).make();
    }



    @Test
    public void indexMapMemCacheOptionTest() throws IOException {
        File dbFile = new File("indexMemCacheTest.db");
        dbFile.delete();
        IdxDB idxDB = IdxDB.newMaker(dbFile).make();
        IndexCollection collectionMemCache = idxDB.newIndexMapBuilder("memCacheIndex").index("key", 1).memCacheSize(100).setMemCacheIndex(true).create();
        IndexCollection collectionNoCache = idxDB.newIndexMapBuilder("noCacheIndex").index("key", 1).memCacheSize(100).setMemCacheIndex(false).create();

        for(int i = 0; i < 1000; ++i) {
            collectionMemCache.add(new CSONObject().put("key", i + "").put("value", i).put("str","sdafjkasdklfjasdklfjasdlfk;jasdlfk;jasdfkl;asdfjasdl;fkhjasdj;dfasjklf"));
            collectionNoCache.add(new CSONObject().put("key", i + "").put("value", i).put("str","sdafjkasdklfjasdklfjasdlfk;jasdlfk;jasdfkl;asdfjasdl;fkhjasdj;dfasjklf"));
        }
        collectionMemCache.commit();
        collectionNoCache.commit();
        System.out.println("초기화완료");

        long start = System.currentTimeMillis();
        for(int i = 50000; i < 100000; ++i) {
            collectionMemCache.add(new CSONObject().put("key", i + "").put("value", i).put("str","1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;"));
            collectionMemCache.commit();
        }
        collectionMemCache.removeByIndex("1");
        collectionMemCache.removeByIndex("50000");
        collectionMemCache.removeByIndex("100000");
        collectionMemCache.commit();

        for(int i = 1; i < 100000; ++i) {
            collectionMemCache.findOneByIndex(i + "");
        }

        long timeLap1 = System.currentTimeMillis() - start + 1;
        System.out.println("인덱스 값 메모리 캐쉬:" + timeLap1 + "ms");

        start = System.currentTimeMillis();
        for(int i = 50000; i < 100000; ++i) {
            collectionNoCache.add(new CSONObject().put("key", i + "").put("value", i).put("str","1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;asdfjasdflkjsdfklasdjflasd;fjasdl;fjasdfjl;asdfjl;"));
            collectionNoCache.commit();
        }
        collectionNoCache.removeByIndex("1");
        collectionNoCache.removeByIndex("50000");
        collectionNoCache.removeByIndex("100000");
        collectionNoCache.commit();

        for(int i = 1; i < 100000; ++i) {
            collectionNoCache.findOneByIndex(i + "");
        }

        long timeLap2 = System.currentTimeMillis() - start + 1;
        System.out.println("인덱스 값 노 캐쉬:"+ timeLap2 + "ms");


        System.out.println("DBFile size: " + (dbFile.length() / (1024 * 1024)) + "mb");

        System.out.println("인덱스 값 메모리 캐쉬:" + timeLap1 + "ms");

        assertTrue(timeLap2 >  timeLap1);

        idxDB = IdxDB.newMaker(dbFile).make();

        dbFile.delete();
    }
}
