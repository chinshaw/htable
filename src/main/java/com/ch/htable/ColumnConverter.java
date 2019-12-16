package com.ch.htable;

//import com.apc.its.fido.ds.client.AnyCollection;
//import com.apc.its.fido.ds.client.Matrix;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkState;

public interface ColumnConverter<T> {

    @FunctionalInterface
    interface ToBytes<T> {
        byte[] apply(T value) throws Exception;
    }

    @FunctionalInterface
    interface FromBytes<T> {
        T apply(byte[] bytes) throws Exception;
    }

    // you should use
    @Deprecated
    byte[] toBytes(T value);

    // You should use fromBytes(Class<T> clazz, byte[] bytes);
    @Deprecated
    T fromBytes(byte[] bytes);

    byte[] toBytes(Class<T> clazz, T value);

    T fromBytes(Class<T> clazz, byte[] bytes);

    class BaseConverter<T> implements ColumnConverter<T> {

        private final ToBytes<T> toBytes;
        private final FromBytes<T> fromBytes;

        BaseConverter(ToBytes<T> toBytes,
                             FromBytes<T> fromBytes) {
            this.toBytes = toBytes;
            this.fromBytes = fromBytes;
        }

        @Override
        public byte[] toBytes(T value) {
            try {
                if (value == null) {
                    return null;
                }
                return toBytes.apply(value);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to call tobytes on %s", value), e);
            }
        }

        @Override
        public T fromBytes(byte[] bytes) {
            try {
                checkState(bytes.length > 0, "attempt to convert entity with no bytes");
                return fromBytes.apply(bytes);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to call fromBytes"), e);
            }
        }

        @Override
        public byte[] toBytes(Class<T> clazz, T value) {
            return toBytes(value);
        }

        @Override
        public T fromBytes(Class<T> clazz, byte[] bytes) {
            return fromBytes(bytes);
        }
    }
    
    class StringColumn extends BaseConverter<String> {
        StringColumn() {
            super(Bytes::toBytes, Bytes::toString);
        }
    }

    class IntegerColumn extends BaseConverter<Integer> {
        public IntegerColumn() {
            super(Bytes::toBytes, Bytes::toInt);
        }
    }

    class LongColumn extends BaseConverter<Long> {
        public LongColumn() {
            super(Bytes::toBytes, Bytes::toLong);
        }
    }

    class DoubleColumn extends BaseConverter<Double> {
        public DoubleColumn() {
            super(Bytes::toBytes, Bytes::toDouble);
        }
    }


//    class MatrixParser extends BaseConverter<Matrix> {
//        MatrixParser() {super(Matrix::toByteArray, Matrix::parseFrom);}
//    }
//
//    class AnyCollectionParser extends BaseConverter<AnyCollection> {
//        AnyCollectionParser() {super(AnyCollection::toByteArray, AnyCollection::parseFrom); }
//    }

    class JacksonJsonConverter<T> implements ColumnConverter<T> {

        protected static final ObjectMapper mapper = new ObjectMapper();
        {
            SimpleModule module = new SimpleModule();
            module.addSerializer(BigDecimal.class, new ToStringSerializer());
            mapper.registerModule(module);
        }


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

        // Don't use these
        public byte[] toBytes(T value) {
            return toBytes((Class<T>) value.getClass(), value);
        }

        /**
         * This is not very efficient and shouldn't be used.
         * @param bytes
         * @return
         */
        public T fromBytes(byte[] bytes) {
            throw new RuntimeException("No worky, use T fromBytes(Class<T> type, byte[] bytes);");
        }
    }

//
//    class BaseCollectionConverter<T> implements ColumnConverter<Collection<T>> {
//
//        private final Supplier<? extends Collection> supplier;
//        private final ToBytes<T> toBytes;
//        private final FromBytes<T> fromBytes;
//
//        public BaseCollectionConverter(Supplier<? extends Collection> supplier, ToBytes<T> toBytes,
//                                       FromBytes<T> fromBytes) {
//            this.supplier = supplier;
//            this.toBytes = toBytes;
//            this.fromBytes = fromBytes;
//        }
//
//        @Override
//        public byte[] toBytes(Collection<T> value) {
//            final ByteBuffer bb = ByteBuffer.allocate(value.size() * Double.SIZE);
//            value.stream().peek(toBytes::apply);
//            return bb.array();
//        }
//
//        @Override
//        public Collection<T> fromBytes(byte[] bytes) {
//            final Collection collection = supplier.get();
//            final ByteBuffer bb = ByteBuffer.wrap(bytes);
//            while (bb.hasRemaining()) {
//                collection.add(bb.getDouble());
//            }
//            return collection;
//        }
//    }

//    /**
//     * Double collection converter.
//     */
//    class DoubleCollectionColumn extends BaseCollectionConverter<Double> {
//        public DoubleCollectionColumn(Supplier<? extends Collection> supplier,
//                                      ToBytes<Double> toBytes, FromBytes<Double> fromBytes) {
//            super(ArrayList::new, Bytes::toBytes, Bytes::toDouble);
//        }
//    }

    /**
     //     * String collection converter.
     //     */
//    class StringCollectionColumn extends BaseCollectionConverter<String> {
//        public StringCollectionColumn(Supplier<? extends Collection> supplier,
//                                      ToBytes<String> toBytes, FromBytes<String> fromBytes) {
//            super(ArrayList::new, Bytes::toBytes, Bytes::toString);
//        }
//    }
//
//    /**
//     * Integer collection converter.
//     */
//    class IntCollectionColumn extends BaseCollectionConverter<Integer> {
//        public IntCollectionColumn(Supplier<? extends Collection> supplier,
//                                      ToBytes<Integer> toBytes, FromBytes<Integer> fromBytes) {
//            super(ArrayList::new, Bytes::toBytes, Bytes::toInt);
//        }
//    }

//
}
