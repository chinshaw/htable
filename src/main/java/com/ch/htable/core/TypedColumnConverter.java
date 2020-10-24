package com.ch.htable.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public interface TypedColumnConverter<T> extends ColumnConverter<T> {

    byte[] toBytes(Class<T> type, T value);

    T fromBytes(Class<T> type, byte[] bytes);

    /**
     * This is not very efficient and shouldn't be used.
     * @return
     */
    default byte[] toBytes(T value) {
        return toBytes((Class<T>) value.getClass(), value);
    }

    /**
     * This is not very efficient and shouldn't be used.
     * @param bytes
     * @return
     */
    default T fromBytes(byte[] bytes) {
        throw new RuntimeException("No worky, use T fromBytes(Class<T> type, byte[] bytes);");
    }

    class JacksonJsonConverter<T> implements TypedColumnConverter<T> {

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public byte[] toBytes(Class<T> type, T value) {
            try {
                return mapper.writeValueAsBytes(value);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(String.format("Unable to json serialize class type %s", type));
            }
        }

        @Override
        public T fromBytes(Class<T> type, byte[] bytes) {
            try {
                return mapper.readValue(bytes, type);
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("Unable to json serialize class type %s", type));
            }
        }
    }

}
