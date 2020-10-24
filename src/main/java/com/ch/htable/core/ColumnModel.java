package com.ch.htable.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


class ColumnModel {

    private final Map<String, ColumnMeta<?>> namedColumns;
    private ColumnMeta<?> anyColumn;

    ColumnModel() {
        namedColumns = new HashMap<>();
    }

    ColumnMeta getNamedColumn(String columnName) {
        return namedColumns.get(columnName);
    }

    Collection<ColumnMeta<?>> getNamedColumns() {
        return namedColumns.values();
    }

    boolean hasColumn(String columnName) {
        return namedColumns.containsKey(columnName);
    }

    void addColumn(String columnName, ColumnMeta<?> column) {
        namedColumns.put(columnName, column);
    }

    /**
     * This is a generic column for all unmapped (named) columns. This allows for a dynamic way to map columns
     * into a map.
     * @param anyColumn The column meta data for the unmapped column.
     */
    void setAnyColumn(ColumnMeta<?> anyColumn) {
        this.anyColumn = anyColumn;
    }

    Optional<ColumnMeta<?>> getColumnOrAny(String columnName) {
        return Optional.ofNullable(namedColumns.getOrDefault(columnName, anyColumn));
    }

    Optional<ColumnMeta<?>> getAnyColumn() {
        return Optional.ofNullable(anyColumn);
    }

    public int size() {
        return ((anyColumn != null) ? 1 : 0) + namedColumns.size();
    }

    boolean hasAnyColumn() {
        return anyColumn != null;
    }
}
