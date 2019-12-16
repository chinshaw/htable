package com.ch.htable;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

interface ValueAccessor<T> {

    T getValue(Object o);

    void setValue(Object o, T value);

    Class<T> getterType();

    Class<T> setterType();

    class MethodAccessor<T> implements ValueAccessor<T> {

        private Method getter;
        private Method setter;

        MethodAccessor() { }

        void setGetter(Method getter) {
            this.getter = getter;
        }

        void setSetter(Method setter) {
            this.setter = setter;
        }

        @Override
        public T getValue(Object o) {
            checkNotNull(o, "Invalid argument for accessor");
            checkState(getter != null, "Missing getter method accessor");
            try {
                return (T) getter.invoke(o);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(String.format("Unable to get value on object %s with method %s ", o, getter), e);
            }
        }

        @Override
        public void setValue(Object o, T value) {
            try {
                setter.invoke(o, value);
            } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
                throw new RuntimeException(String.format("Unable to set value on object %s with method %s ", o, getter), e);
            }
        }

        @Override
        public Class<T> getterType() {
            if (getter == null || getter.getReturnType() == Void.class) {
                throw new IllegalStateException("You have a column marked with a void return type");
            }
            return (Class<T>) getter.getReturnType();
        }

        public Class<T> setterType() {
            if (setter.getParameterCount() != 1) {
                throw new IllegalStateException(String.format("Setters must have exactly one argument:  %s", setter));
            }
            Class<T> clazz = (Class<T>) setter.getParameterTypes()[0];
            if (AnyColumn.class.isAssignableFrom(clazz)) {
                clazz =  (Class<T>) ((ParameterizedType)setter.getGenericParameterTypes()[0]).getActualTypeArguments()[0];
            }
            return clazz;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MethodAccessor.class.getSimpleName() + "[", "]")
                    .add("getter=" + getter)
                    .add("setter=" + setter)
                    .toString();
        }
    }
}
