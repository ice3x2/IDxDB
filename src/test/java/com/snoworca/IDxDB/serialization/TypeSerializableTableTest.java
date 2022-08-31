package com.snoworca.IDxDB.serialization;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


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
        assertTrue(equalCollections(originTestClass.vBoolList, testClass.vBoolList));
        assertTrue(equalCollections(originTestClass.vsCollection, testClass.vsCollection));
        assertTrue(equalCollections(originTestClass.vcHashSet, testClass.vcHashSet));
        assertTrue(equalCollections(originTestClass.viTreeSet, testClass.viTreeSet));
        assertTrue(equalCollections(originTestClass.vfSortedSet, testClass.vfSortedSet));
        assertTrue(equalCollections(originTestClass.vlVector, testClass.vlVector));
        assertTrue(equalCollections(originTestClass.vdStack, testClass.vdStack));
        assertTrue(equalCollections(originTestClass.vStrLinkedQueue, testClass.vStrLinkedQueue));

    }

    public boolean equalCollections(Collection<?> a1, Collection<?> a2) {
        if(a1.size() != a2.size()) return false;
        Iterator<?> a1Iter = a1.iterator();
        Iterator<?> a2Iter = a2.iterator();
        while(a1Iter.hasNext()) {
            Object a1v = a1Iter.next();
            Object a2v = a2Iter.next();
            if(!Objects.deepEquals(a1v, a2v)) return false;
        }
        return true;
    }






    @Serializable(name =  "TEST", version = 1)
    public static class TestClass implements DataSerializable {


        @Column
        boolean vBool;
        @Column
        byte vb;
        @Column
        short vs;
        @Column
        char vc;
        @Column
        int vi;
        @Column
        float vf;
        @Column
        long vl;
        @Column
        double vd;
        @Column
        byte[] vBuffer;
        @Column
        String vStr;


        @Column
        boolean[] vBools;
        @Column
        short[] vss;
        @Column
        char[] vcs;
        @Column
        int[] vis;
        @Column
        float[] vfs;
        @Column
        long[] vls;
        @Column
        double[] vds;
        @Column
        String[] vStrs;



        @Column
        List<Boolean> vBoolList;
        @Column
        Collection<Short> vsCollection;
        @Column
        HashSet<Character> vcHashSet;
        @Column
        TreeSet<Integer> viTreeSet;
        @Column
        SortedSet<Float> vfSortedSet;
        @Column
        Vector<Long> vlVector;
        @Column
        Stack<Double> vdStack;
        @Column
        ConcurrentLinkedDeque<String> vStrLinkedQueue;



        @Column
        String[] checkNullStrs;
        @Column
        String checkNullStr;

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

            vBools = new boolean[rand.nextInt(256)];
            for(int i = 0; i < vBools.length; ++i) vBools[i] = rand.nextBoolean();
            vss = new short[rand.nextInt(256)];
            for(int i = 0; i < vss.length; ++i) vss[i] = (short) rand.nextInt();
            vcs = new char[rand.nextInt(256)];
            for(int i = 0; i < vcs.length; ++i) vcs[i] = (char) rand.nextInt();
            vis = new int[rand.nextInt(256)];
            for(int i = 0; i < vis.length; ++i) vis[i] = rand.nextInt();
            vfs = new float[rand.nextInt(256)];
            for(int i = 0; i < vfs.length; ++i) vfs[i] = rand.nextFloat();
            vls = new long[rand.nextInt(256)];
            vds = new double[rand.nextInt(256)];
            for(int i = 0; i < vls.length; ++i) vls[i] = rand.nextLong();
            for(int i = 0; i < vds.length; ++i) vds[i] = rand.nextDouble();
            vStrs = new String[rand.nextInt(256)];
            for(int i = 0; i < vStrs.length; ++i) vStrs[i] = makeRandomString(4, 10);

            rand.nextBytes(vBuffer);
            vBoolList = new LinkedList<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vBoolList.add(rand.nextBoolean());
            }

            vsCollection = new LinkedList<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vsCollection.add((short) rand.nextInt());
            }

            vcHashSet = new HashSet<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vcHashSet.add((char)rand.nextInt());
            }

            viTreeSet = new TreeSet<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                viTreeSet.add(rand.nextInt());
            }

            vfSortedSet = new TreeSet<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                viTreeSet.add(rand.nextInt());
            }

            vlVector = new Vector<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vlVector.add(rand.nextLong());
            }

            vdStack = new Stack<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vdStack.add(rand.nextDouble());
            }

            vStrLinkedQueue = new ConcurrentLinkedDeque<>();
            for(int i = 0, n = rand.nextInt(256); i < n; ++i) {
                vStrLinkedQueue.add(makeRandomString(4, 14));
            }


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
                        Arrays.equals(target.vBuffer, this.vBuffer) &&
                        Arrays.equals(target.vBools, this.vBools) &&
                        Arrays.equals(target.vss, this.vss) &&
                        Arrays.equals(target.vcs, this.vcs) &&
                        Arrays.equals(target.vis, this.vis) &&
                        Arrays.equals(target.vfs, this.vfs) &&
                        Arrays.equals(target.vds, this.vds) &&
                        Arrays.equals(target.vStrs, this.vStrs) &&
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
                    .putDouble(vd).putByteArray(vBuffer).putString(vStr).putInteger(vi);
        }

        @Override
        public void migrate(long previousVersion, ByteBuffer previousBuffer, long newVersion, ByteBuffer newBuffer) throws Exception {

        }
    }

}