package com.snoworca.IdxDB.util;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class CusLinkedHashMapTest {

    @Test
    public void orderTest() {
        CusLinkedHashMap<String, Integer> map = new CusLinkedHashMap<>(true);
        for(Integer value : map.values(false)) {
            System.out.println(value);
        }


        for(int i = 0; i < 10; ++i) {
            map.put(i + "", i);
        }



        for(Integer value : map.values(false)) {
            System.out.println(value);
        }
        System.out.println();
        System.out.println();
        for(Integer value : map.values(true)) {
            System.out.println(value);
        }

        System.out.println();
        System.out.println();
        map.get("5");

        for(Integer value : map.values(false)) {
            System.out.println(value);
        }
        System.out.println();
        System.out.println();

        Iterator<Map.Entry<String, Integer>> entryIterator  = map.entrySet(true).iterator();
        while(entryIterator.hasNext()) {
             Map.Entry<String, Integer> entry  = entryIterator.next();
             System.out.println(entry.getValue());
             if(entry.getValue() == 4) {
                 entryIterator.remove();;
             }
        }

        map.put("0", 0);

        System.out.println();
        System.out.println();

        for(Integer value : map.values(true)) {
            System.out.println(value);
        }



    }

}