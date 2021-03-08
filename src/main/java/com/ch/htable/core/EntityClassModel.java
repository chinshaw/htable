package com.ch.htable.core;

import com.ch.htable.HBaseException;
import com.ch.htable.annotations.HColumn;
import com.ch.htable.annotations.HId;
import com.ch.htable.annotations.HTable;
import com.google.common.base.MoreObjects;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkState;

/**
 * @param <T> Class type of the backing entity.
 */
class EntityClassModel<T> {

    private final Class<T> clazz;
    private final String tableName;
    private final String columnFamily;
    private final ColumnModel columnModel;

    private Identifier identifier;

    EntityClassModel(Class<T> clazz, String tableName, String columnFamily) {
        this.clazz = clazz;
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnModel = new ColumnModel();
    }

    public ColumnModel getColumnModel() {
        return columnModel;
    }

    Collection<ColumnMeta<?>> getNamedColumns() {
        return columnModel.getNamedColumns();
    }

    /**
     * Add a column to the list of columns. This will use the column name as the key in the map
     * and the column meta object as it's value.
     * @param columnName The name of the column
     * @param column The meta instance for the column.
     */
    void addColumn(String columnName, ColumnMeta<?> column) {
        columnModel.addColumn(columnName, column);
    }


    void setAnyColumn(ColumnMeta<?> column) {
        this.columnModel.setAnyColumn(column);
    }

    ColumnMeta<?> getAnyColumn() {
        return this.columnModel.getAnyColumn().get();
    }

    boolean hasAnyColumn() {
        return this.columnModel.getAnyColumn().isPresent();
    }

    /**
     * Check to see if the entity model knows about a column.
     * @param columnName The string name of the column.
     * @return true if column exists in cache.
     */
    boolean hasColumn(String columnName) {
        return columnModel.hasColumn(columnName);
    }

    /**
     * Getter for a single column by it's column name. The column name is typically the name
     * marked in the {@link HColumn} annotation.
     * @see #hasColumn(String)
     * @param columnName The name of the column to lookup.
     * @return ColumnMeta instance if found.
     */
    ColumnMeta<?> getColumnOrAny(String columnName) {
        return columnModel.getColumnOrAny(columnName)
                .orElseThrow(() ->
                        new HBaseException(String.format("Failed to find column by name %s for entity %s", columnName, getEntityType())));
    }

    ColumnMeta<?> getColumnOrThrow(String columnName) {
        if (! columnModel.hasColumn(columnName)) {
            throw new HBaseException(String.format("Failed to find column by name %s for entity %s", columnName, getEntityType()));
        }
        return columnModel.getNamedColumn(columnName);
    }

    /**
     * Getter for the name of the table. This is marked on the entity by it's {@link HTable} annotation. This is
     * the hbase table name for the entity.
     * @return String table name.
     */
    String getTableName() {
        return tableName;
    }

    /**
     * The concrete entity type of the mapped entity.
     * @return The implementing class for the entity.
     */
    public Class<T> getEntityType() {
        return clazz;
    }

    /**
     * Set the identifier accessor for the row.
     * @param identifier The identifier for the row.
     */
    public void setIdentifier(Identifier identifier) {
        this.identifier = identifier;
    }

    /**
     * Get the identifier accessor.
     * @return
     */
    public Identifier getIdentifier() {
        return identifier;
    }

    /**
     * Get the column family for the
     * @return
     */
    public String getColumnFamily() {
        return columnFamily;
    }

    /**
     * Helper to get the byte value for an entity using it's Identifier implementation.
     * @param o The value to invoke the identifier on.
     * @return
     */
    byte[] getIdValue(Object o) {
        return identifier.converter.toBytes(identifier.valueAccessor.getValue(o));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("tableName", tableName)
                .toString();
    }


    /**
     * An identifier is an combination of a column converter and value accessor. This is created when the
     * entity meta model finds a {@link HId} marked method in the entity. This is essentially the way we will
     * create a row value in hbase.
     */
    static class Identifier {

        private final ColumnConverter converter;
        private final ValueAccessor valueAccessor;

        Identifier(ColumnConverter converter, ValueAccessor valueAccessor) {
            this.converter = converter;
            this.valueAccessor = valueAccessor;
        }
    }
}
