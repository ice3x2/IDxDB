package com.snoworca.IDxDB.serialization;


import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Serializable {
    String value() default "";
    String name() default  "";
    long version() default  0;
    int bufferSize() default  1024;
}
