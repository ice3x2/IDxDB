package com.snoworca.IdxDB;

import com.snoworca.IdxDB.collection.IndexTree;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class IdxDBTest {


    @Test
    public void memCacheTest() {
        File file = new File("test.db");
        IdxDB idxDB = IdxDB.create(file);
        IndexTree set = idxDB.newSetBuilder("208300").index("dateL", 1).memCacheSize(1000).create();

        ArrayList<JSONObject> testDatas = new ArrayList<>();
        for(int i = 0; i < 2000; ++i) {
            JSONObject jsonObject = new JSONObject().put("CODE", i % 100).put("dateL", i);
            testDatas.add(jsonObject);
        }
        Collections.shuffle(testDatas);
        for(int i = 0; i < 2000; ++i) {
            set.add(testDatas.get(i));
        }
        assertTrue(set.size() == 1000);


        int lastValue = -1;
        for(JSONObject jsonObject : set) {
            assertTrue(Math.abs(jsonObject.getInt("dateL") - lastValue) == 1);
            lastValue = jsonObject.getInt("dateL");
           // System.out.println(lastValue);
        }

        Iterator<JSONObject> iterator =  set.iterator();
        iterator.next();
        iterator.remove();
        JSONObject jsonObject = iterator.next();
        assertEquals(1, jsonObject.get("dateL"));
        assertEquals(999, set.size());


        set.add(new JSONObject().put("CODE",0).put("dateL", 0));
        set.add(new JSONObject().put("CODE",220).put("dateL", 0));
        set.add(new JSONObject().put("CODE",4000).put("dateL", 4000));
        JSONObject testObj = set.last();
        assertEquals(999,testObj.get("dateL"));

        //set.replace(new JSONObject().put("CODE","1234555").put("dateL", 555));
        //assertEquals("1234555", set.findByIndex(555).get("CODE"));
        file.delete();
    }


    @Test
    public void queryTest() {
        File file = new File("test.db");
        IdxDB idxDB = IdxDB.create(file);
        JSONObject resultQuery = null;

        // 이름 없음 에러
        JSONObject createSetErrorQueryNoName = new JSONObject().put("method", "createSet").put("argument",new JSONObject().put("index", new JSONObject().put("dateL", -1)).put("limit", 1000));
        resultQuery = idxDB.executeQuery(createSetErrorQueryNoName);
        System.out.print("이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 인덱스가 없음 에러.
        JSONObject createSetErrorQueryNoIndex = new JSONObject().put("method", "createSet").put("argument",new JSONObject().put("name", "201120").put("limit", 1000).put("index", new JSONObject()));
        resultQuery = idxDB.executeQuery(createSetErrorQueryNoIndex);
        System.out.print("인덱스가 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 정상 생성
        JSONObject createSetQuery = new JSONObject().put("method", "createSet").put("argument",new JSONObject().put("name", "201120").put("index", new JSONObject().put("dateL", -1)).put("limit", 1000));
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
        JSONObject addQueryNoSetError = new JSONObject().put("method", "add").put("argument",new JSONObject().put("name", "sdafasdf").put("data", new JSONObject().put("dateL", 100).put("name", "삼성전자")));
        resultQuery = idxDB.executeQuery(addQueryNoSetError);
        System.out.print("콜렉션 이름 없음: ");
        System.out.println(resultQuery);
        assertTrue(resultQuery.optBoolean("isError"));


        // 두 번째 쿼리는 무시됨.
        JSONObject addQuery = new JSONObject().put("method", "add").put("argument", new JSONObject().put("name","201120").put("data", new JSONObject().put("dateL", 100).put("name", "삼성전자")));
        resultQuery = idxDB.executeQuery(addQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("데이터 삽입: ");
        System.out.println(resultQuery);
        addQuery = new JSONObject().put("method", "add").put("argument", new JSONObject().put("name","201120").put("data", new JSONObject().put("dateL", 100).put("name", "HLB")));
        resultQuery = idxDB.executeQuery(addQuery);
        assertFalse(resultQuery.optBoolean("isError"));
        assertFalse(resultQuery.optBoolean("success"));
        System.out.print("중복 인덱스 삽입: ");
        System.out.println(resultQuery);

        JSONObject findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("검색결과(eq): ");
        System.out.println(resultQuery);
        assertEquals("삼성전자",resultQuery.optJSONArray("data").getJSONObject(0).optString("name"));




        // add or replace
        JSONObject addOrReplaceQuery = new JSONObject().put("method", "addOrReplace").put("argument", new JSONObject().put("name","201120").put("data", new JSONObject().put("dateL", 100).put("name", "HLB")));
        resultQuery = idxDB.executeQuery(addOrReplaceQuery);

        findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",100).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("addOrReplace 호출후 검색결과(eq): ");
        System.out.println(resultQuery);
        assertEquals("HLB",resultQuery.optJSONArray("data").getJSONObject(0).optString("name"));



        ArrayList<JSONObject> testDatas = new ArrayList<>();
        for(int i = 0; i < 200; ++i) {
            JSONObject jsonObject = new JSONObject().put("CODE", i % 100).put("dateL", i);
            testDatas.add(jsonObject);
        }
        Collections.shuffle(testDatas);
        JSONArray addArray = new JSONArray();
        for(int i = 0; i < 200; ++i) {
            addArray.put(testDatas.get(i));
        }

        // add or replace All
        JSONObject addOrReplaceAllQuery = new JSONObject().put("method", "addOrReplace").put("argument", new JSONObject().put("name","201120").put("data", addArray));
        resultQuery = idxDB.executeQuery(addOrReplaceAllQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("addOrReplaceAllQuery: ");
        System.out.println(resultQuery);

        findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("limit",30).put("where",new JSONObject().put("dateL",50).put("$op", "gte")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find gte(50), limit(30): ");
        System.out.println(resultQuery);
        JSONArray resultData = resultQuery.getJSONArray("data");
        assertEquals(30, resultData.length());
        assertEquals(resultData.getJSONObject(0).getInt("dateL"), 50);
        assertEquals(resultData.getJSONObject(29).getInt("dateL"), 79);


        findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("limit",5).put("where",new JSONObject().put("dateL",50).put("$op", "gt")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find gt(50), limit(5): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getJSONArray("data");
        assertEquals(5, resultData.length());
        assertEquals(resultData.getJSONObject(0).getInt("dateL"), 51);
        assertEquals(resultData.getJSONObject(4).getInt("dateL"), 55);


        findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("limit",7).put("where",new JSONObject().put("dateL",10).put("$op", "lte")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find lte(10), limit(7): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getJSONArray("data");
        assertEquals(7, resultData.length());
        assertEquals(resultData.getJSONObject(0).getInt("dateL"), 10);
        assertEquals(resultData.getJSONObject(6).getInt("dateL"), 4);


        findQuery = new JSONObject().put("method", "findByIndex").put("argument", new JSONObject().put("name","201120").put("limit",100000).put("where",new JSONObject().put("dateL",30).put("$op", "lt")));
        resultQuery = idxDB.executeQuery(findQuery);
        System.out.print("find lt(30), limit(100000): ");
        System.out.println(resultQuery);
        resultData = resultQuery.getJSONArray("data");
        assertEquals(30, resultData.length());
        assertEquals(resultData.getJSONObject(0).getInt("dateL"), 29);
        assertEquals(resultData.getJSONObject(29).getInt("dateL"), 0);


        // remove
        JSONObject removeQuery = new JSONObject().put("method", "removeByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",150).put("$op", "lt")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optJSONArray("data").length(), 150);


        removeQuery = new JSONObject().put("method", "removeByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",150).put("$op", "lte")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optJSONArray("data").length(), 1);

        removeQuery = new JSONObject().put("method", "removeByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",151).put("$op", "eq")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(resultQuery.optJSONArray("data").length(), 1);


        removeQuery = new JSONObject().put("method", "removeByIndex").put("argument", new JSONObject().put("name","201120").put("where",new JSONObject().put("dateL",191).put("$op", "gte")));
        resultQuery = idxDB.executeQuery(removeQuery);
        assertTrue(resultQuery.optBoolean("success"));
        System.out.print("remove: ");
        System.out.println(resultQuery);
        assertEquals(9,resultQuery.optJSONArray("data").length());

        // 크기 가져오기
        JSONObject sizeQuery = new JSONObject().put("method", "size").put("argument", new JSONObject().put("name","201120"));
        resultQuery = idxDB.executeQuery(sizeQuery);
        System.out.print("크기: ");
        System.out.println(resultQuery.optInt("data"));

        int size = resultQuery.optInt("data");


        JSONObject getQuery = new JSONObject().put("method", "list").put("argument", new JSONObject().put("name","201120"));
        resultQuery = idxDB.executeQuery(getQuery);
        System.out.print("list: ");
        System.out.println(resultQuery.optJSONArray("data"));
        assertEquals(resultQuery.optJSONArray("data").length(), size);


        getQuery = new JSONObject().put("method", "list").put("argument", new JSONObject().put("name","201120").put("limit", 5).put("revers", true));
        resultQuery = idxDB.executeQuery(getQuery);
        System.out.print("revers list: ");
        System.out.println(resultQuery.optJSONArray("data"));


        file.delete();








    }

}