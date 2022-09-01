package com.snoworca.IDxDB.scheme;

import com.snoworca.IDxDB.serialization.Column;
import com.snoworca.IDxDB.serialization.Serializable;

import static org.junit.jupiter.api.Assertions.*;

class SchemesTest {



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


    public static class TestClassC2 extends  TestClassC1 {
        @Column("adc")
        String abc;

    }

}

