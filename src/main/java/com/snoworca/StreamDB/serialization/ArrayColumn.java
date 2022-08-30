package com.snoworca.StreamDB.serialization;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ArrayColumn {
    String name() default "";
    int maxSize();
    boolean cutOverSize() default true;
}
