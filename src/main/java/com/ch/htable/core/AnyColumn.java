package com.ch.htable.core;

/**
 * Anycolumn is a simple class that allows an entity implementation to handle hbase column values.
 * That are returned but not implicitly mapped. In a case where we have scenario outputs it would be
 * ridiculous and virtually impossible to name all the possible values. With an @HAnyColumn setter method any
 * column that is not implcitly mapped will be called with an AnyColumn value containing the name of the column
 * along with the actual value. The entity can specify how it wants to handle those columns.
 * @param <T>
 */
public class AnyColumn<T> {

    private String columnName;

    public T value;

    public AnyColumn(String columnName, T value) {
        this.columnName = columnName;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public static <T> T of(String name, T value) {
        return (T) new AnyColumn(name, value);
    }
}
