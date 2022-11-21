package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.FindOption;
import com.snoworca.IdxDB.collection.IndexCollection;
import com.snoworca.IdxDB.collection.IndexLinkedMap;
import com.snoworca.IdxDB.collection.IndexTreeSet;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class IdxDBTest {


    @Test
    public void restoreOnThreadTest() throws IOException {
        File file = new File("test.db");
        file.delete();
        IdxDB idxDB = IdxDB.newMaker(file).setRestoreOnMultiThead(true).make();
        for(int i = 0; i < 500; ++i) {
            IndexCollection indexCollection = idxDB.newIndexTreeSetBuilder("test" + i).index("idx",1).create();
            ArrayList<Integer> list = new ArrayList<>();
            for(int j = 0; j < 1000; ++j) {
                list.add(j);
            }
            Collections.shuffle(list);
            for(int j = 0; j < 1000; ++j) {
                indexCollection.add(new CSONObject().put("idx", list.get(j)));
            }
            indexCollection.commit();
        }
        idxDB.close();
        long start = System.currentTimeMillis();
        idxDB = IdxDB.newMaker(file).setRestoreOnMultiThead(false).make();
        //assertEquals(1000, idxDB.collectionSize());
        long labTimeSync = System.currentTimeMillis() - start;
        idxDB.close();
        start = System.currentTimeMillis();
        idxDB = IdxDB.newMaker(file).setRestoreOnMultiThead(true).make();
        //assertEquals(2600, idxDB.collectionSize());
        long labTimeAsync = System.currentTimeMillis() - start;
        idxDB.close();
        System.out.println("Sync: " + labTimeSync + "ms");
        System.out.println("Async: " + labTimeAsync + "ms");
        assertTrue(labTimeAsync < labTimeSync);

        file.delete();



    }

    @Test
    public void memCacheTest() throws IOException {
        File file = new File("memCache.db");
        file.delete();
        IdxDB idxDB = IdxDB.newMaker(file).make();
        IndexTreeSet set = idxDB. newIndexTreeSetBuilder("208300").index("dateL", 1).memCacheSize(1000).create();


        ArrayList<CSONObject> testDatas = new ArrayList<>();
        for(int i = 0; i < 2000; ++i) {
            CSONObject jsonObject = new CSONObject().put("CODE", i % 100).put("dateL", i);
            testDatas.add(jsonObject);
        }
        Collections.shuffle(testDatas);
        for(int i = 0; i < 2000; ++i) {
            set.add(testDatas.get(i));
        }
        set.commit();


        int lastValue = -1;
        for(CSONObject jsonObject : set) {
            assertTrue(Math.abs(jsonObject.getInteger("dateL") - lastValue) == 1);
            lastValue = jsonObject.getInteger("dateL");
        }

        Iterator<CSONObject> iterator =  set.iterator();
        iterator.next();
        iterator.remove();
        CSONObject jsonObject = iterator.next();
        assertEquals(1, jsonObject.get("dateL"));
        assertEquals(1999, set.size());

        set.add(new CSONObject().put("CODE",0).put("dateL", 0));
        set.add(new CSONObject().put("CODE",220).put("dateL", 0));
        set.add(new CSONObject().put("CODE",4000).put("dateL", 4000));
        set.commit();
        CSONObject testObj = set.last();
        assertEquals(4000,testObj.get("dateL"));

        //set.replace(new CSONObject().put("CODE","1234555").put("dateL", 555));
        //assertEquals("1234555", set.findByIndex(555).get("CODE"));
        file.delete();
    }


    @Test
    public void fileStoreTest() throws IOException {
        File file = new File("fileStoreTest.dat");
        file.delete();
        long start = System.currentTimeMillis();
        IdxDB idxDB = IdxDB.newMaker(file).make();
        int collectionSize = 20;
        int rowSize = 10000;
        int memCacheSize = 1000;
        long testCase = collectionSize * rowSize;


        for(int k = 0; k < collectionSize; ++k) {
            IndexCollection indexCollection;
            if(k == 5) {
                indexCollection = idxDB.newIndexMapBuilder(k + "").memCacheSize(memCacheSize).index("date", 1).create();
            } else if(k == 6) {
                indexCollection = idxDB.newIndexMapBuilder(k +"").memCacheSize(memCacheSize).setAccessOrder(true).index("date" , -1).create();
            } else {
                indexCollection = idxDB. newIndexTreeSetBuilder(k +"").setFileStore(true).memCacheSize(memCacheSize).index("date" , 1).create();
            }

            for(int i = 0; i < rowSize; ++i) {
                indexCollection.add(new CSONObject().put("date", i).put("str", i + ""));
            }
            indexCollection.commit();
            if(k % 10 == 0 && k != 0) {
                System.out.println(k + ",");
            } else {
                System.out.print(k + ", ");
            }

        }
        System.out.println(testCase + " 개 쓰기 " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();
        List<CSONObject> csonObjectListOriList = idxDB.get("0").list(Integer.MAX_VALUE, false);
        for(CSONObject csonObject : csonObjectListOriList) {
            if(csonObject.optInteger("date") % 10 == 0) {
                idxDB.get("0").removeByIndex(csonObject.optInteger("date"), FindOption.fromOP(OP.eq));
            }
        }
        idxDB.get("0").commit();

        idxDB.get("2").removeByIndex(2000, FindOption.fromOP(OP.gte));
        idxDB.get("2").commit();

        idxDB.get("3").clear();
        idxDB.get("3").commit();

        assertFalse(idxDB.dropCollection("21"));
        assertTrue(idxDB.dropCollection("19"));

        assertEquals(rowSize, csonObjectListOriList.size());
        System.out.println( rowSize + "개 읽기 " + (System.currentTimeMillis() - start) + "ms");

        idxDB.close();

        start = System.currentTimeMillis();
        idxDB = IdxDB.newMaker(file).make();
        System.out.println( testCase + " 개 로딩 " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();
        List<CSONObject> csonObjectList = idxDB.get("1").list(Integer.MAX_VALUE, false);
        assertEquals(rowSize, csonObjectList.size());
        System.out.println( rowSize + " 개 읽기 " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();
        csonObjectList = idxDB.get("1").list(memCacheSize, false);
        System.out.println("메모리에 캐쉬된 " + memCacheSize + "개 읽기 " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();
        csonObjectList = idxDB.get("0").list(Integer.MAX_VALUE, false);
        assertEquals(rowSize - (10000 / 10), csonObjectList.size());
        System.out.println(csonObjectList.size() + "개 읽기 " + (System.currentTimeMillis() - start) + "ms");


        start = System.currentTimeMillis();
        csonObjectList = idxDB.get("2").list(Integer.MAX_VALUE, false);
        assertEquals(2000, csonObjectList.size());
        System.out.println(csonObjectList.size() +  "개 읽기 " + (System.currentTimeMillis() - start) + "ms");


        start = System.currentTimeMillis();
        csonObjectList = idxDB.get("3").list(Integer.MAX_VALUE, false);
        assertEquals(0, csonObjectList.size());
        System.out.println(csonObjectList.size() +  "개 읽기 " + (System.currentTimeMillis() - start) + "ms");


        assertEquals(null, idxDB.get("19"));


        //indexCollection.findByIndex(5, FindOption)

        IndexLinkedMap indexLinkedMap = (IndexLinkedMap)idxDB.get("5");
        CSONObject result = indexLinkedMap.findOneByIndex(1);
        assertEquals(result.optString("str"), "1");


        start = System.currentTimeMillis();
        List<CSONObject>  list = indexLinkedMap.list(1000, false);
        System.out.println((System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();
        list = indexLinkedMap.list(1000, true);
        System.out.println((System.currentTimeMillis() - start) + "ms");


        indexLinkedMap = (IndexLinkedMap)idxDB.get("6");
        result = indexLinkedMap.findOneByIndex(5000);
        assertEquals(result.optString("str"), "5000");
        result = indexLinkedMap.findOneByIndex(4000);
        result = indexLinkedMap.findOneByIndex(3000);
        result = indexLinkedMap.findOneByIndex(2000);
        indexLinkedMap.commit();


        start = System.currentTimeMillis();
        list = indexLinkedMap.list(1000, false);
        System.out.println( (System.currentTimeMillis() - start) + "ms");
        assertTrue((System.currentTimeMillis() - start) < 2);
        assertEquals("2000",list.get(0).get("str"));
        assertEquals("3000",list.get(1).get("str"));
        assertEquals("4000",list.get(2).get("str"));
        assertEquals("5000",list.get(3).get("str"));
        System.out.println((System.currentTimeMillis() - start) + "ms");



        file.delete();



    }

    @Test
    public void queryTest() throws IOException {
        File file = new File("test.db");
        file.delete();
        IdxDB idxDB = IdxDB.newMaker(file).make();
        CSONObject resultQuery = null;

        // 이름 없음 에러
        CSONObject createSetErrorQueryNoName = new CSONObject().put("method", "newIndexTreeSet").put("argument",new CSONObject().put("index", new CSONObject().put("dateL", -1)).put("memCacheSize", 1000));
        resultQuery = idxDB.executeCSONQuery(createSetErrorQueryNoName);
        System.out.print("이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 인덱스가 없음 에러.
        CSONObject createSetErrorQueryNoIndex = new CSONObject().put("method", "newIndexTreeSet").put("argument",new CSONObject().put("collection", "201120").put("memCacheSize", 1000).put("index", new CSONObject()));
        resultQuery = idxDB.executeCSONQuery(createSetErrorQueryNoIndex);
        System.out.print("인덱스가 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 정상 생성
        CSONObject createSetQuery = new CSONObject().put("method", "newIndexTreeSet").put("argument",new CSONObject().put("collection", "201120").put("index", new CSONObject().put("dateL", -1)).put("memCacheSize", 1000));
        resultQuery = idxDB.executeCSONQuery(createSetQuery);
        System.out.print("생성 성공: ");
        System.out.println(resultQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertTrue(resultQuery.optBoolean("success"));

        // 이미 존재 에러.
        resultQuery = idxDB.executeCSONQuery(createSetQuery);
        System.out.print("같은 이름 이미 존재: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));

        // collection 이름 없음 에러
        CSONObject addQueryNoSetError = new CSONObject().put("method", "add").put("argument",new CSONObject().put("collection", "sdafasdf").put("data", new CSONObject().put("dateL", 100).put("collection", "삼성전자")));
        resultQuery = idxDB.executeCSONQuery(addQueryNoSetError);
        System.out.print("콜렉션 이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 두 번째 쿼리는 무시됨.
        CSONObject addQuery = new CSONObject().put("method", "add").put("argument", new CSONObject().put("collection","201120").put("data", new CSONObject().put("dateL", 100).put("collection", "삼성전자")));
        resultQuery = idxDB.executeCSONQuery(addQuery);
        System.out.print("데이터 삽입: ");
        System.out.println(resultQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertTrue(resultQuery.optBoolean("success"));

        addQuery = new CSONObject().put("method", "add").put("argument", new CSONObject().put("collection","201120").put("data", new CSONObject().put("dateL", 100).put("collection", "HLB")));
        resultQuery = idxDB.executeCSONQuery(addQuery);
        assertFalse(resultQuery.optBoolean("isError"));


        CSONObject findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("검색결과(eq) - 중복 추가 무시: ");
        System.out.println(resultQuery);
        assertEquals("삼성전자",resultQuery.optArray("data").getObject(0).optString("collection"));




        // add or replace
        CSONObject addOrReplaceQuery = new CSONObject().put("method", "addOrReplace").put("argument", new CSONObject().put("collection","201120").put("data", new CSONObject().put("dateL", 100).put("collection", "HLB")));
        resultQuery = idxDB.executeCSONQuery(addOrReplaceQuery);

        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("addOrReplace 호출후 검색결과(eq): ");
        System.out.println(resultQuery);
        assertEquals("HLB",resultQuery.optArray("data").getObject(0).optString("collection"));



        ArrayList<CSONObject> testDatas = new ArrayList<>();
        for(int i = 0; i < 200; ++i) {
            CSONObject jsonObject = new CSONObject().put("CODE", i % 100).put("dateL", i);
            testDatas.add(jsonObject);
        }
        Collections.shuffle(testDatas);
        CSONArray addArray = new CSONArray();
        for(int i = 0; i < 200; ++i) {
            addArray.add(testDatas.get(i));
        }

        // add or replace All
        CSONObject addOrReplaceAllQuery = new CSONObject().put("method", "addOrReplace").put("argument", new CSONObject().put("collection","201120").put("data", addArray));
        resultQuery = idxDB.executeCSONQuery(addOrReplaceAllQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("addOrReplaceAllQuery: ");
        System.out.println(resultQuery);

        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("limit",30).put("where",new CSONObject().put("dateL",50).put("$op", "gte")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("find gte(50), limit(30): ");
        System.out.println(resultQuery);
        CSONArray resultData = resultQuery.getArray("data");
        assertEquals(30, resultData.size());
        assertEquals(resultData.getObject(0).getInteger("dateL"), 50);
        assertEquals(resultData.getObject(29).getInt("dateL"), 79);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("limit",5).put("where",new CSONObject().put("dateL",50).put("$op", "gt")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("find gt(50), limit(5): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(5, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 51);
        assertEquals(resultData.getObject(4).getInt("dateL"), 55);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("limit",7).put("where",new CSONObject().put("dateL",10).put("$op", "lte")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("find lte(10), limit(7): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(7, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 10);
        assertEquals(resultData.getObject(6).getInt("dateL"), 4);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("collection","201120").put("limit",100000).put("where",new CSONObject().put("dateL",30).put("$op", "lt")));
        resultQuery = idxDB.executeCSONQuery(findQuery);
        System.out.print("find lt(30), limit(100000): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(30, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 29);
        assertEquals(resultData.getObject(29).getInt("dateL"), 0);


        // remove
        CSONObject removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",150).put("$op", "lt")));
        resultQuery = idxDB.executeCSONQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.getObject("result").getInt("remove"), 150);


        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",150).put("$op", "lte")));
        resultQuery = idxDB.executeCSONQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.getObject("result").getInt("remove"), 1);

        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",151).put("$op", "eq")));
        resultQuery = idxDB.executeCSONQuery(removeQuery);
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("success"));
        assertEquals(resultQuery.getObject("result").getInt("remove"), 1);


        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",191).put("$op", "gte")));
        resultQuery = idxDB.executeCSONQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(9,resultQuery.getObject("result").getInt("remove"));


        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("collection","201120").put("where",new CSONObject().put("dateL",123123123).put("$op", "gte")));
        resultQuery = idxDB.executeCSONQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(0,resultQuery.getObject("result").getInt("remove"));

        // 크기 가져오기
        CSONObject sizeQuery = new CSONObject().put("method", "size").put("argument", new CSONObject().put("collection","201120"));
        resultQuery = idxDB.executeCSONQuery(sizeQuery);
        System.out.print("크기: ");
        System.out.println(resultQuery.optInt("data"));

        int size = resultQuery.optInt("data");


        CSONObject getQuery = new CSONObject().put("method", "list").put("argument", new CSONObject().put("collection","201120"));
        resultQuery = idxDB.executeCSONQuery(getQuery);
        System.out.print("list: ");
        System.out.println(resultQuery.optArray("data"));
        assertEquals(resultQuery.optArray("data").size(), size);


        getQuery = new CSONObject().put("method", "list").put("argument", new CSONObject().put("collection","201120").put("memCacheSize", 5).put("revers", true));
        resultQuery = idxDB.executeCSONQuery(getQuery);
        System.out.print("revers list: ");
        System.out.println(resultQuery.optArray("data"));


        file.delete();

    }

    @Test
    public void replaceDataSetTest() throws IOException {
        File file = new File("test.db");
        file.delete();
        IdxDB db = IdxDB.newMaker(file).make();
        IndexTreeSet indexTreeSet = db.newIndexTreeSetBuilder("testDB").index("key", 1).setCapacityRatio(0.5f).create();
        indexTreeSet.add(new CSONObject().put("key", "1234567890").put("value", "BBBBBBBBBB"));
        indexTreeSet.commit();
        long fileSize = file.length();
        indexTreeSet.addOrReplace(new CSONObject().put("key", "1234567890").put("value", "AAAAAAAAAAAAAA"));
        System.out.println(indexTreeSet.commit().toString());
        assertEquals(fileSize, file.length());

        assertEquals("AAAAAAAAAAAAAA", indexTreeSet.findOneByIndex("1234567890").get("value"));


        indexTreeSet.addOrReplace(new CSONObject().put("key", "1234567890").put("value", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCc"));
        indexTreeSet.commit();

        assertNotEquals(fileSize, file.length());

        assertEquals("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCc", indexTreeSet.findOneByIndex("1234567890").get("value"));

    }


    @Test
    public void replaceDataMapTest() throws IOException {
        File file = new File("test.db");
        file.delete();
        IdxDB db = IdxDB.newMaker(file).make();
        IndexLinkedMap indexLinkedMap = db.newIndexMapBuilder("testDB").index("key", 1).setCapacityRatio(0.5f).create();
        indexLinkedMap.add(new CSONObject().put("key", "1234567890").put("value", "BBBBBBBBBB"));
        indexLinkedMap.commit();
        long fileSize = file.length();
        indexLinkedMap.addOrReplace(new CSONObject().put("key", "1234567890").put("value", "AAAAAAAAAAAAAA"));
        System.out.println(indexLinkedMap.commit().toString());
        assertEquals(fileSize, file.length());

        assertEquals("AAAAAAAAAAAAAA", indexLinkedMap.findOneByIndex("1234567890").get("value"));


        indexLinkedMap.addOrReplace(new CSONObject().put("key", "1234567890").put("value", "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCc"));
        indexLinkedMap.commit();

        assertNotEquals(fileSize, file.length());

        assertEquals("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCc", indexLinkedMap.findOneByIndex("1234567890").get("value"));
        file.delete();

    }

    /*
    @Test
    public void compressionTest() throws IOException {
        Random rand = new Random(System.currentTimeMillis());

        File file = new File("test.db");
        file.delete();
        IdxDB noneDB = IdxDB.newMaker(file).compressionType(CompressionType.NONE).make();
        IndexTreeSet indexTreeSetNone = noneDB.newIndexTreeSetBuilder("testDB").index("key", 1).create();
        for(long i = 0; i < 100000L; ++i) {
            CSONObject data = new CSONObject().put("key", i);
            indexTreeSetNone.add(data);
            for(int c = 'a'; c <= 'z'; ++c) {
                data.put(String.valueOf((char)c), rand.nextLong());
            }
        }
        long start = System.currentTimeMillis();
        indexTreeSetNone.commit();
        System.out.println("압축하지 않음: " + (file.length() / 1024 / 1024) + "Mb   " + (System.currentTimeMillis() - start) + "ms" );
        noneDB.close();
        file.delete();


        IdxDB gzipDB = IdxDB.newMaker(file).compressionType(CompressionType.GZIP).make();
        IndexTreeSet indexTreeSetGZIP = gzipDB.newIndexTreeSetBuilder("testDB").index("key", 1).create();
        for(long i = 0; i < 100000L; ++i) {
            CSONObject data = new CSONObject().put("key", i);
            indexTreeSetGZIP.add(data);
            for(int c = 'a'; c <= 'z'; ++c) {
                data.put(String.valueOf((char)c), rand.nextLong());
            }
        }
        start = System.currentTimeMillis();
        indexTreeSetGZIP.commit();
        System.out.println("Gzip 압축: " + (file.length() / 1024 / 1024) + "Mb  " + (System.currentTimeMillis() - start) + "ms" );
        gzipDB.close();
        file.delete();


        IdxDB deflateDB = IdxDB.newMaker(file).compressionType(CompressionType.Deflater).make();
        IndexTreeSet indexTreeSetDeflate = deflateDB.newIndexTreeSetBuilder("testDB").index("key", 1).create();
        for(long i = 0; i < 100000L; ++i) {
            CSONObject data = new CSONObject().put("key", i);
            indexTreeSetDeflate.add(data);
            for(int c = 'a'; c <= 'z'; ++c) {
                data.put(String.valueOf((char)c), rand.nextLong());
            }
        }
        start = System.currentTimeMillis();
        indexTreeSetDeflate.commit();
        System.out.println("Deflate 압축: " + (file.length() / 1024 / 1024) + "Mb  " + (System.currentTimeMillis() - start) + "ms" );
        deflateDB.close();
        file.delete();

        IdxDB snappyDB = IdxDB.newMaker(file).compressionType(CompressionType.SNAPPY).make();
        IndexTreeSet indexTreeSetSnappy = snappyDB.newIndexTreeSetBuilder("testDB").index("key", 1).create();
        for(long i = 0; i < 100000L; ++i) {
            CSONObject data = new CSONObject().put("key", i);
            indexTreeSetSnappy.add(data);
            for(int c = 'a'; c <= 'z'; ++c) {
                data.put(String.valueOf((char)c), rand.nextLong());
            }
        }
        start = System.currentTimeMillis();
        indexTreeSetSnappy.commit();
        System.out.println("Snappy 압축: " + (file.length() / 1024 / 1024) + "Mb  " + (System.currentTimeMillis() - start) + "ms" );
        snappyDB.close();

        snappyDB = IdxDB.newMaker(file).compressionType(CompressionType.SNAPPY).make();
        file.delete();
    }*/



}