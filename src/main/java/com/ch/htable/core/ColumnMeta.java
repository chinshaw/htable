package com.ch.htable.core;

import com.ch.htable.annotations.HColumn;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Column Meta is information about the column used during serialization and
 * deserialization. These are stored in the case and used to access values on a mapped object.
 *
 * @param <T> The actual value type for the column. This is essentially the getter/setter value.
 */
class ColumnMeta<T> {
    private String name;
    protected ValueAccessor<T> valueAccessor;
    protected ColumnConverter<T> converter;
    private byte[] qualifier;

    ColumnMeta(String name) {
        setName(name);
    }

    ColumnMeta(String name, ColumnConverter converter) {
        setName(name);
        this.converter = converter;
    }

    /**
     * The name is the field name of the column. This is derived from the {@link HColumn} annotation.
     * In the future this could be easily deciphered from the name of the getter field.
     *
     * @return The string name of the column
     */
    String getName() {
        return name;
    }

    /**
     * @param name The name of the column set from the HColumn annotation.
     * @see #getName()
     */
    void setName(String name) {
        this.name = name;
        this.qualifier = Bytes.toBytes(name);
    }

    /**
     * The qualifier is the quick lookup of the column name. When set name is called this will initialize the
     * qualifier to bytes so that it can be cached quickly rather than running toBytes on ever call.
     *
     * @return The byte[] array qualifier for the hbase column.
     */
    byte[] getQualifier() {
        return qualifier;
    }

    /**
     * The column converter will handle converting the value to bytes and frombytes.
     *
     * @return The column converter created when the ColumnMeta was created.
     */
    ColumnConverter<T> converter() {
        return converter;
    }

    /**
     * @param converter
     * @see #setConverter(ColumnConverter)
     */
    void setConverter(ColumnConverter<T> converter) {
        this.converter = converter;
    }

    /**
     * Value is essentially a method that can be called to get or set the field on the object.
     * The {@link ValueAccessor#getValue(Object)} is called when retrieving the value for serialization.
     * The {@link ValueAccessor#setValue(Object, Object)} is used when deserializing and object from bytes and settting
     * the value on an object.
     *
     * @return The value accessor.
     * @see #getBytes(Object)
     * @see #setBytes(Object, byte[], String)
     */
    ValueAccessor<T> getValueAccessor() {
        return valueAccessor;
    }

    /**
     * @param valueAccessor
     * @see #getValueAccessor()
     */
    void setValueAccessor(ValueAccessor<T> valueAccessor) {
        this.valueAccessor = valueAccessor;
    }

    /**
     * Helper method that can be used to get the field from and object and convert it to
     * bytes to be stored in a column. This takes a pojo object and will invoke the value accessor to get the
     * mapped field and convert it to bytes.
     *
     * @param entity The pojo entity that will have the valueAccessor called on.
     * @return
     */
    byte[] getBytes(Object entity) {
        return converter.toBytes(valueAccessor.getterType(), valueAccessor.getValue(entity));
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
        valueAccessor.setValue(entity, converter.fromBytes(valueAccessor.setterType(), bytes));
    }
}
