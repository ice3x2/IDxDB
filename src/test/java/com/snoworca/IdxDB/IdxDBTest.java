package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.FindOption;
import com.snoworca.IdxDB.collection.IndexTree;
import com.snoworca.cson.CSONArray;
import com.snoworca.cson.CSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IdxDBTest {


    @Test
    public void memCacheTest() throws IOException {
        File file = new File("test.db");
        file.delete();
        IdxDB idxDB = IdxDB.newMaker(file).make();
        IndexTree set = idxDB.newIndexTreeBuilder("208300").index("dateL", 1).memCacheSize(1000).create();

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
           // System.out.println(lastValue);
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
        CSONObject testObj = set.last();
        assertEquals(4000,testObj.get("dateL"));

        //set.replace(new CSONObject().put("CODE","1234555").put("dateL", 555));
        //assertEquals("1234555", set.findByIndex(555).get("CODE"));
        file.delete();
    }


    @Test
    public void fileStoreTest() throws IOException {
        File file = new File("fileStoreTest.dat");
        long start = System.currentTimeMillis();
        IdxDB idxDB = IdxDB.newMaker(file).make();
        int collectionSize = 20;
        int rowSize = 10000;
        int memCacheSize = 1000;
        long testCase = collectionSize * rowSize;


        for(int k = 0; k < collectionSize; ++k) {
            IndexTree indexTree = idxDB.newIndexTreeBuilder(k +"").setFileStore(true).memCacheSize(memCacheSize).index("date" , 1).create();
            for(int i = 0; i < rowSize; ++i) {
                indexTree.add(new CSONObject().put("date", i).put("str", i + ""));
            }
            indexTree.commit();
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




        file.delete();



    }

    @Test
    public void queryTest() throws IOException {
        File file = new File("test.db");
        IdxDB idxDB = IdxDB.newMaker(file).make();
        CSONObject resultQuery = null;

        // 이름 없음 에러
        CSONObject createSetErrorQueryNoName = new CSONObject().put("method", "createSet").put("argument",new CSONObject().put("index", new CSONObject().put("dateL", -1)).put("limit", 1000));
        resultQuery = idxDB.executeQuery(createSetErrorQueryNoName);
        System.out.print("이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 인덱스가 없음 에러.
        CSONObject createSetErrorQueryNoIndex = new CSONObject().put("method", "createSet").put("argument",new CSONObject().put("name", "201120").put("limit", 1000).put("index", new CSONObject()));
        resultQuery = idxDB.executeQuery(createSetErrorQueryNoIndex);
        System.out.print("인덱스가 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 정상 생성
        CSONObject createSetQuery = new CSONObject().put("method", "createSet").put("argument",new CSONObject().put("name", "201120").put("index", new CSONObject().put("dateL", -1)).put("limit", 1000));
        resultQuery = idxDB.executeQuery(createSetQuery);
        System.out.print("생성 성공: ");
        System.out.println(resultQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertTrue(resultQuery.optBoolean("success"));

        // 이미 존재 에러.
        resultQuery = idxDB.executeQuery(createSetQuery);
        System.out.print("같은 이름 이미 존재: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));

        // collection 이름 없음 에러
        CSONObject addQueryNoSetError = new CSONObject().put("method", "add").put("argument",new CSONObject().put("name", "sdafasdf").put("data", new CSONObject().put("dateL", 100).put("name", "삼성전자")));
        resultQuery = idxDB.executeQuery(addQueryNoSetError);
        System.out.print("콜렉션 이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 두 번째 쿼리는 무시됨.
        CSONObject addQuery = new CSONObject().put("method", "add").put("argument", new CSONObject().put("name","201120").put("data", new CSONObject().put("dateL", 100).put("name", "삼성전자")));
        resultQuery = idxDB.executeQuery(addQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("데이터 삽입: ");
        System.out.println(resultQuery);
        addQuery = new CSONObject().put("method", "add").put("argument", new CSONObject().put("name","201120").put("data", new CSONObject().put("dateL", 100).put("name", "HLB")));
        resultQuery = idxDB.executeQuery(addQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertFalse(resultQuery.optBoolean("success"));
        System.out.print("중복 인덱스 삽입: ");
        System.out.println(resultQuery);

        CSONObject findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("검색결과(eq): ");
        System.out.println(resultQuery);
        assertEquals("삼성전자",resultQuery.optArray("data").getObject(0).optString("name"));




        // add or replace
        CSONObject addOrReplaceQuery = new CSONObject().put("method", "addOrReplace").put("argument", new CSONObject().put("name","201120").put("data", new CSONObject().put("dateL", 100).put("name", "HLB")));
        resultQuery = idxDB.executeQuery(addOrReplaceQuery);

        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("addOrReplace 호출후 검색결과(eq): ");
        System.out.println(resultQuery);
        assertEquals("HLB",resultQuery.optArray("data").getObject(0).optString("name"));



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
        CSONObject addOrReplaceAllQuery = new CSONObject().put("method", "addOrReplace").put("argument", new CSONObject().put("name","201120").put("data", addArray));
        resultQuery = idxDB.executeQuery(addOrReplaceAllQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("addOrReplaceAllQuery: ");
        System.out.println(resultQuery);

        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("limit",30).put("where",new CSONObject().put("dateL",50).put("$op", "gte")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find gte(50), limit(30): ");
        System.out.println(resultQuery);
        CSONArray resultData = resultQuery.getArray("data");
        assertEquals(30, resultData.size());
        assertEquals(resultData.getObject(0).getInteger("dateL"), 50);
        assertEquals(resultData.getObject(29).getInt("dateL"), 79);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("limit",5).put("where",new CSONObject().put("dateL",50).put("$op", "gt")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find gt(50), limit(5): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(5, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 51);
        assertEquals(resultData.getObject(4).getInt("dateL"), 55);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("limit",7).put("where",new CSONObject().put("dateL",10).put("$op", "lte")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find lte(10), limit(7): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(7, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 10);
        assertEquals(resultData.getObject(6).getInt("dateL"), 4);


        findQuery = new CSONObject().put("method", "findByIndex").put("argument", new CSONObject().put("name","201120").put("limit",100000).put("where",new CSONObject().put("dateL",30).put("$op", "lt")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find lt(30), limit(100000): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getArray("data");
        assertEquals(30, resultData.size());
        assertEquals(resultData.getObject(0).getInt("dateL"), 29);
        assertEquals(resultData.getObject(29).getInt("dateL"), 0);


        // remove
        CSONObject removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",150).put("$op", "lt")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optArray("data").size(), 150);


        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",150).put("$op", "lte")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optArray("data").size(), 1);

        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",151).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optArray("data").size(), 1);


        removeQuery = new CSONObject().put("method", "removeByIndex").put("argument", new CSONObject().put("name","201120").put("where",new CSONObject().put("dateL",191).put("$op", "gte")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(9,resultQuery.optArray("data").size());

        // 크기 가져오기
        CSONObject sizeQuery = new CSONObject().put("method", "size").put("argument", new CSONObject().put("name","201120"));
        resultQuery = idxDB.executeQuery(sizeQuery);
        System.out.print("크기: ");
        System.out.println(resultQuery.optInt("data"));

        int size = resultQuery.optInt("data");


        CSONObject getQuery = new CSONObject().put("method", "list").put("argument", new CSONObject().put("name","201120"));
        resultQuery = idxDB.executeQuery(getQuery);
        System.out.print("list: ");
        System.out.println(resultQuery.optArray("data"));
        assertEquals(resultQuery.optArray("data").size(), size);


        getQuery = new CSONObject().put("method", "list").put("argument", new CSONObject().put("name","201120").put("limit", 5).put("revers", true));
        resultQuery = idxDB.executeQuery(getQuery);
        System.out.print("revers list: ");
        System.out.println(resultQuery.optArray("data"));


        file.delete();








    }

}