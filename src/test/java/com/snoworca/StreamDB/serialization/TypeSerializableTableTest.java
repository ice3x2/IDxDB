package com.snoworca.StreamDB.serialization;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;


class TypeSerializableTableTest {


    private static String makeRandomString(int minLength, int maxLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = ThreadLocalRandom.current().nextInt(maxLength - minLength) + minLength;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)(random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        String generatedString = buffer.toString();
        return generatedString;
    }

    @Test
    public void serializedSerializeTes() throws Exception {
        TypeSerializableTable<TestClass> table = TypeSerializableTable.newTable(TestClass.class);
        TestClass originTestClass = new TestClass();
        ByteBuffer buffer = table.serialize(originTestClass);
        ArrayList<ByteBuffer> bufferList = new ArrayList<>();
        bufferList.add(buffer);
        TestClass testClass = table.deserialize(bufferList);
        assertEquals(originTestClass.vStr, testClass.vStr);
        assertEquals(originTestClass, testClass);
    }




    @Serializable(name =  "TEST", version = 1)
    public static class TestClass implements DataSerializable {


        @PrimitiveColumn
        boolean vBool;
        @PrimitiveColumn
        byte vb;
        @PrimitiveColumn
        short vs;
        @PrimitiveColumn
        char vc;
        @PrimitiveColumn
        int vi;
        @PrimitiveColumn
        float vf;
        @PrimitiveColumn
        long vl;
        @PrimitiveColumn
        double vd;
        //@PrimitiveField
        byte[] vBuffer;
        //@PrimitiveField("string")

        @StringColumn(maxSize = 2000, cutOverSize = false)
        String vStr;

        public TestClass() {
            Random rand =ThreadLocalRandom.current();
            vBool = rand.nextBoolean();
            vb = (byte) rand.nextInt(256);
            vs = (short) rand.nextInt(Short.MAX_VALUE);
            vc = (char)rand.nextInt(Short.MAX_VALUE);
            vi = rand.nextInt();
            vf = rand.nextFloat();
            vl = rand.nextLong();
            vd = rand.nextDouble();
            vBuffer = new byte[rand.nextInt(1024) + 10];
            rand.nextBytes(vBuffer);
            this.vStr = makeRandomString(1, 2000);

        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof TestClass) {
                TestClass target = (TestClass)obj;
                return target.vBool == this.vBool &&
                target.vb == this.vb &&
                target.vs == this.vs  &&
                target.vc == this.vc  &&
                target.vi == this.vi &&
                        target.vf == this.vf &&
                        target.vl == this.vl &&
                        target.vd == this.vd &&
                       // Arrays.equals(target.vBuffer, this.vBuffer) &&
                        this.vStr.equals(target.vStr);
            }
            return false;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public long getType() {
            return 0;
        }

        @Override
        public void deserialize(Deserializer deserializer) {

        }

        @Override
        public void serialize(Serializer serializer) throws Exception {
            serializer.putBoolean(vBool).putByte(vb).putCharacter(vc).putShort(vs).putInteger(vi).putFloat(vf).putLong(vl)
                    .putDouble(vd).putByteArray(vBuffer).putString(vStr, 100).putInteger(vi);
        }

        @Override
        public void migrate(long previousVersion, ByteBuffer previousBuffer, long newVersion, ByteBuffer newBuffer) throws Exception {

        }
    }

}