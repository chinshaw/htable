package com.ch.htable;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({FIELD, METHOD})
public @interface HCollection {

    String name();

    Class<? extends ColumnConverter<Collection>> converter();
}
