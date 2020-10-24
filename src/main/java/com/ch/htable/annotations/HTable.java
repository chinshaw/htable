package com.ch.htable.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * Used to mark an hbase entity. This is useful for specifiying the name of the table along with
 * the column family. By default the column family is specified as "default". This is the default
 * column family name in hbase.
 */
@Retention(RUNTIME)
@Target({TYPE})
public @interface HTable {

    String name();

    String cf() default "default";
}
