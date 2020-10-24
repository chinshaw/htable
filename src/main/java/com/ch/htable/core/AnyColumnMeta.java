package com.ch.htable.core;


class AnyColumnMeta<T> extends ColumnMeta<AnyColumn<T>> {

    AnyColumnMeta(String name) {
        super(name);
    }

    AnyColumnMeta(String name, ColumnConverter converter) {
        super(name, converter);
    }

    /**
     * Helper method to set a value on an object from an hbase column value. This will invoke the setter on the entity
     * parameter after converting the value from bytes.
     *
     * @param entity  The entity that will have it's setter invoked.
     * @param bytes   The hbase bytes[] column value to set.
     * @param colName
     */
    void setBytes(Object entity, byte[] bytes, String colName) {
        valueAccessor.setValue(entity, AnyColumn.of(colName, converter.fromBytes(valueAccessor.setterType(), bytes)));
//        valueAccessor.setValue(entity, converter.fromBytes(valueAccessor.setterType(), bytes));
    }
}
