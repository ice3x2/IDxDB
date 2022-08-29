package com.snoworca.StreamDB;

import com.snoworca.StreamDB.serialization.DataSerializable;
import com.snoworca.StreamDB.serialization.Deserializer;
import com.snoworca.StreamDB.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class StreamDBTest {




    @Test
    public void initTest() {
        //StreamDB streamDB = StreamDB.open("./test.db");
        //StreamList<TestClass> list = streamDB.listBuilder("map1").setBlockSize(1028 * 1024).create();


    }


    public static class TestClass implements DataSerializable {

        String name = "";
        long value = 0;

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public long getType() {
            return 1;
        }

        @Override
        public void deserialize(Deserializer buffer) {

        }

        @Override
        public void serialize(Serializer buffer) throws Exception {

        }


        @Override
        public void migrate(long previousVersion, ByteBuffer previousBuffer, long newVersion, ByteBuffer newBuffer) throws Exception {

        }
    }

}