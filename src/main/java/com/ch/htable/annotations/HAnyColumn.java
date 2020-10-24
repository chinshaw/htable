package com.ch.htable.annotations;

import com.ch.htable.core.ColumnConverter;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This can be used to mark a getter, setter pair to be used when a column
 * is not found. This essentially is a generic bucket to store everything else in.
 */
@Retention(RUNTIME)
@Target({FIELD, METHOD})
public @interface HAnyColumn {
    Class<? extends ColumnConverter> converter();
}
