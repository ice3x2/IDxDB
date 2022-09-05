package com.snoworca.IDxDB.scheme;

import com.snoworca.IDxDB.exception.UnserializableTypeException;
import com.snoworca.IDxDB.serialization.Column;
import com.snoworca.IDxDB.serialization.Serializable;
import com.snoworca.IDxDB.serialization.SerializableTypeTable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemesTest {


    boolean reloadTest = false;
    byte[] commitBuffer = null;
    Schemes schemes = null;

    @Test
    public void schemeTest() throws NoSuchFieldException {
        if(!reloadTest) {
            schemes = Schemes.newInstance(new Schemes.CommitLoadDelegator() {
                @Override
                public void onCommit(byte[] buffer) {
                    commitBuffer = buffer;
                }

                @Override
                public byte[] getSchemeBuffer() {
                    return commitBuffer;
                }

            });

            schemes.newScheme(TestClassA.class);
            schemes.newScheme(TestClassB.class);
            schemes.newScheme(TestClassC.class);
            boolean isError = false;
            try {
                schemes.newScheme(TestClassC1.class);
            } catch (UnserializableTypeException e) {
                isError = true;
            }
            assertTrue(isError);
            schemes.newScheme(TestClassC2.class);

        } else {
            schemes.clear();
            schemes.load();
        }

        schemes.commit();


        SerializableTypeTable tableA = schemes.getTableByClassName(TestClassA.class.getName());
        assertEquals(TestClassA.class, tableA.getType());
        tableA = schemes.getTableByName(TestClassA.class.getName());
        assertEquals(TestClassA.class, tableA.getType());
        SerializableTypeTable tableB = schemes.getTableByClassName(TestClassB.class.getName());
        assertEquals(TestClassB.class, tableB.getType());
        tableB = schemes.getTableByName("B");
        assertEquals(TestClassB.class, tableB.getType());

        SerializableTypeTable tableC2 = schemes.getTableByClassName(TestClassC2.class.getName());
        assertEquals(TestClassC2.class, tableC2.getType());
        tableC2 = schemes.getTableByName("C2");
        assertEquals(TestClassC2.class, tableC2.getType());


        assertEquals(TestClassC2.class.getDeclaredField("abc"), tableC2.findFieldByName("adc1"));



        schemes.commit();
        if(!reloadTest) {
            reloadTest = true;
            schemeTest();
        }

    }


    @Serializable
    public static class TestClassA {

    }

    @Serializable(name = "B")
    public static class TestClassB  {

    }

    @Serializable(name = "C")
    public static class TestClassC {
        @Column
        String abc;

    }

    public static class TestClassC1 extends  TestClassC {
        @Column
        String abc;
    }


    @Serializable(name = "C2")
    public static class TestClassC2 extends  TestClassC1 {
        @Column("adc1")
        String abc;
        @Column
        int intValue;

    }

}

