package com.ch.htable;

import com.google.common.annotations.Beta;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.ch.htable.LambdaFunctionUtil.uncheck;
import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Iterables.partition;
import static java.util.stream.StreamSupport.stream;


public class HEntityManager {

    private static final Logger LOG = LoggerFactory.getLogger(HEntityManager.class);

    private static final int MAX_BATCH_SIZE = 0xFFF;
    private final AnnotationAnalyzer metaModel;
    private final String tableNameSpace;
    private final Connection connection;

    public HEntityManager(Connection connection, AnnotationAnalyzer metaModel, String tableNameSpace) {
        this.connection = connection;
        this.metaModel = metaModel;
        this.tableNameSpace = tableNameSpace;
    }

    /**
     * This is used to save or update a single entity. It will look up the entity in the database
     *
     * @param entity The entity to save.
     * @param <T>    The type of the entity being saved.
     */
    public <T> void save(T entity) {
        final EntityClassModel m = metaModel.entityModel(entity.getClass());
        try (Table table = getTable(m)) {
            final Put row = entityToPut(entity);
            table.put(row);
        } catch (IOException e) {
            throw new HBaseException("Error while saving entity column", e);
        }
    }

    /**
     * Save a collection of entities to the datastore.
     */
    public <T> void saveAll(Iterable<T> entities, Class<T> clazz) {
        final EntityClassModel m = metaModel.entityModel(clazz);

        try (Table table = getTable(m)) {
            stream(partition(entities, MAX_BATCH_SIZE).spliterator(), true)
                    .map((batch) -> entityToPut(m, batch))
                    .forEach(b -> {
                        try {
                            // put the batch
                            table.put(b);
                        } catch (IOException e) {
                            throw new HBaseException("Error while saving entity column", e);
                        }
                    });
        } catch (IOException e) {
            throw new HBaseException("Error while saving entity column", e);
        }
    }

    public <T> T getOne(Class<T> clazz, String key, String ... columns) {
        final EntityClassModel m = metaModel.entityModel(clazz);
        try (final Table table = getTable(m)) {
            final Get get = new Get(Bytes.toBytes(key));
            Stream.of(columns)
                    .forEach(c -> get.addColumn(Bytes.toBytes(m.getColumnFamily()), Bytes.toBytes(c)));

            final Result r = table.get(get);
            if (r.isEmpty()) {
                throw new HBaseException(String.format("Error retrieving entity for class %s with key %s", clazz, key));
            }

            return resultToEntity(r, m);
        } catch (IOException e) {
            throw new HBaseException(String.format("Error retrieving entity for class %s with key %s", clazz, key));
        }
    }

    public <T> Filter singleColumnFilter(Class<T> clazz, String column, CompareFilter.CompareOp compareOp, byte[] value ) {
        final EntityClassModel m = metaModel.entityModel(clazz);
        final ColumnMeta<?> columnDescriptor = m.getColumnOrThrow(column);
        return new SingleColumnValueFilter(Bytes.toBytes(m.getColumnFamily()), columnDescriptor.getQualifier(), compareOp, value);
    }


    public <T> T findFirst(Class<T> clazz, String key) {
        final EntityClassModel<T> m = metaModel.entityModel(clazz);
        final Scan scan = createScan(clazz, key, Optional.empty());
        scan.setMaxResultSize(1);

        try (final Table table = getTable(m)) {
            final ResultScanner scanner = table.getScanner(scan);
            return resultToEntity(scanner.next(), m);
        } catch (IOException e) {
            throw new HBaseException("Error while attempting to find first result", e);
        }
    }

    public <T> Stream<T> find(Class<T> clazz, String prefix, String ... columns) {
        return find(clazz, prefix, Optional.empty(), columns);
    }

    /**
     * Important you must close the stream coming back from this method. This will result in closing the table
     * along with the ResultScanner. If not this will cause a memory leak.
     *
     * @param clazz The class type that will be scanned.
     * @param prefix The filter that will be used for find.
     * @param filter The optional filter that can be applied to the results.
     * @param columns List of columns to fetch.
     * @param <T> The type of entity that will be returned.
     * @return Stream of entities found matching the find.
     */
    public <T> Stream<T> find(Class<T> clazz, String prefix, Optional<Filter> filter, String ... columns) {
        final EntityClassModel<T> m = metaModel.entityModel(clazz);

        try {
            final Table table = getTable(m);
            final Scan scan = createScan(clazz, prefix, filter, columns);

            final ResultScanner scanner = table.getScanner(scan);
            return stream(scanner.spliterator(), true)
                    .onClose(() -> uncheck(() -> {
                        scanner.close();
                        table.close();
                    })).map(r -> resultToEntity(r, m));

        } catch (IOException e) {
            throw new HBaseException(String.format("Error while finding column data for column(s) %s on" +
                    " column family %s and row key prefix %s", columns, m.getColumnFamily(), prefix), e);
        }
    }


    public <T> Stream<String> findRowKeys(Class<T> clazz, String prefix) {
        final EntityClassModel<T> m = metaModel.entityModel(clazz);
        try {

            final Table table = getTable(m);
            final FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    new FirstKeyOnlyFilter(),
                    new KeyOnlyFilter());
            final Scan scan = createScan(clazz, prefix, Optional.of(filters));

            final ResultScanner scanner = table.getScanner(scan);
            return stream(scanner.spliterator(), true)
                    .onClose(() -> uncheck(() -> {
                        scanner.close();
                        table.close();
                    })).map(r -> Bytes.toString(r.getRow()));

        } catch (IOException e) {
            throw new HBaseException(String.format("Error while finding column data for " +
                    " column family %s and row key prefix %s", m.getColumnFamily(), prefix), e);
        }
    }


    /**
     * Create a scan that can be used to query the datastore.
     *
     * @param clazz The type of entity that will be scanned.
     * @param prefix The prefix for the scan.
     * @param filter Any filter that will be applied.
     * @param columns All columns that will be queries. If empty all will be returned.
     * @param <T> The type of entity that will be returned.
     * @return A newly created scan instance.
     */
    protected <T> Scan createScan(Class<T> clazz, String prefix, Optional<Filter> filter, String ... columns) {
        final EntityClassModel<T> m = metaModel.entityModel(clazz);
        final Scan scan = new Scan();

        scan.addFamily(Bytes.toBytes(m.getColumnFamily()));
        scan.setRowPrefixFilter(Bytes.toBytes(prefix));
//        scan.setMaxResultSize(1);

        // Add an optional filter
        filter.ifPresent(scan::setFilter);

        // If we have columns then we can add them.
        for (String col : columns) {
            scan.addColumn(Bytes.toBytes(m.getColumnFamily()), Bytes.toBytes(col));
        }
        return scan;
    }

    /**
     * This is a dirty version that uses the first value to decipher the class for the collection.
     *
     * @param entities List of entities
     * @param <T>      The type of entity that will be saved
     * @return Collection of put objects.
     */
    protected <T> Collection<Put> entityToPut(List<T> entities) {
        final EntityClassModel m = metaModel.entityModel(entities.get(0).getClass());
        return entityToPut(m, entities);
    }

    /**
     * This will construct a list of puts based on a collection of entities. It will convert the entity to a put
     * using the column information from the entity com.apc.its.services.fido.geomodel.
     *
     * @param m        The entity com.apc.its.services.fido.geomodel of the entity.
     * @param entities The Collection of entities that will be converted to puts.
     * @param <T>      The type of the entity that is being passed in.
     * @return List of put instances that are ready to be published to hbase.
     */
    protected static <T> List<Put> entityToPut(EntityClassModel m, Collection<T> entities) {
        return entities.stream().parallel()
                .map(e -> entityToPut(m, e))
                .collect(Collectors.toList());
    }

    /**
     * This will look up the type of entity and use it to delegate to {@link #entityToPut(EntityClassModel, Object)}
     *
     * @param entity The entity object that will be converted to put.
     * @param <T>    The type of the entity.
     * @return Put created from the entity.
     */
    protected <T> Put entityToPut(T entity) {
        final EntityClassModel m = metaModel.entityModel(entity.getClass());
        return entityToPut(m, entity);
    }

    /**
     * Implementation of entity to put that will convert the entity to a put that can be saved into
     * hbase. This will iterate through all the columns and set the values and qualifiers on the put using
     * the entity com.apc.its.services.fido.geomodel.
     * <p>
     * This method will validate that the entity com.apc.its.services.fido.geomodel type and entity class match before proceeding.
     *
     * @param m      The entity com.apc.its.services.fido.geomodel of the entity.
     * @param entity The entity instance that will be converted to the put.
     * @param <T>    The type of the entity.
     * @return Put that was created from the entity com.apc.its.services.fido.geomodel
     */
    protected static <T> Put entityToPut(EntityClassModel m, T entity) {
        checkArgument(m.getEntityType().isAssignableFrom(entity.getClass()),
                "Entity com.apc.its.services.fido.geomodel %s is not assignable to class entity type %s", m.getEntityType(), entity.getClass());
        final ColumnModel cm = m.getColumnModel();
        checkState(cm.size() > 0, "Invalid number of columns for entity %s", entity);
        final Put put = new Put(m.getIdValue(entity));

        cm.getNamedColumns().forEach(c -> {
            if (c.getQualifier() == null || c.getQualifier().length == 0) {
                throw new IllegalStateException(String.format("Entity does not have a valid column identifier for type %s", m.getEntityType()));
            }
            put.addColumn(Bytes.toBytes(m.getColumnFamily()),
                    c.getQualifier(), c.getBytes(entity));
        });

        return put;
    }

    public <T> void delete(T entity) {
        final EntityClassModel m = metaModel.entityModel(entity.getClass());
        delete(m.getEntityType(), Stream.of(m.getIdValue(entity)));
    }

    public <T> void delete(Class<T> clazz, String id) {
        final EntityClassModel m = metaModel.entityModel(clazz);
        delete(m.getEntityType(), Stream.of(Bytes.toBytes(id)));
    }


    public <T> void deleteAll(Class<T> clazz, Stream<T> entityStream) {
        final EntityClassModel m = metaModel.entityModel(clazz);
        try (Table table = getTable(m)) {
            final List<Delete> deletes = entityStream.map(m::getIdValue)
                    .map(Delete::new).collect(Collectors.toList());

            table.delete(deletes);
        } catch (IOException e) {
            throw new HBaseException("Error while deleting row entity", e);
        }
    }


    /**
     * Delete a stream of keys from the data store. This assumes that the keys have already been converted
     * to bytes.
     * Note: this should probably not be exposed and will likely be converted to protected.
     *
     * @param clazz The required entity type for the keys that will be deleted.
     * @param keys Stream of keys to delete.
     */
    @Beta
    public void delete(Class<?> clazz, Stream<byte[]> keys) {
        final EntityClassModel m = metaModel.entityModel(clazz);
        try (Table table = getTable(m)) {
            final List<Delete> deletes = keys.map(Delete::new).collect(Collectors.toList());
            table.delete(deletes);
        } catch (IOException e) {
            throw new HBaseException("Error while deleting row entity", e);
        }
    }

    /**
     * Get the table from the entity com.apc.its.services.fido.geomodel. This will use the @HTable annotation from the com.apc.its.services.fido.geomodel to look up
     * the table. This does have an added check to validate that the table is name is present.
     * @param entityModel The entity com.apc.its.services.fido.geomodel used to decipher the table name.
     * @return The table instance that can be used to make queries.
     */
    private Table getTable(final EntityClassModel entityModel) {
        checkNotNull(entityModel.getTableName(), "Invalid table name for entity type %s ", entityModel.getEntityType());
        try {
            final String table = tableNameSpace + entityModel.getTableName();
            LOG.debug("Creating connection for table: {}", table);
            return connection.getTable(TableName.valueOf(table));
        } catch (IOException e) {
            throw new RuntimeException("Unable to connect initialize table " + entityModel.getTableName(), e);
        }
    }

    /**
     * Convert a result to an entity. This will look at the result and set the fields using the column
     * meta data stored in the entity com.apc.its.services.fido.geomodel. It will automatically convert the value using the value accessor
     * that was registered for the column.
     *
     * @param result Hbase result object.
     * @param m      The entity com.apc.its.services.fido.geomodel for the entity
     * @param <T>    The type of the entity that will be returned.
     * @return A newly constructed entity with values set.
     */
    private static <T> T resultToEntity(Result result, EntityClassModel m) {
        checkState(! result.isEmpty(), "No results were found while attempting to map entity for model %s", m);
        final T instance;
        try {
            instance = (T) m.getEntityType().newInstance();
            for (Cell c : result.rawCells()) {
                final String colName = Bytes.toString(CellUtil.cloneQualifier(c));
                final ColumnMeta<?> col = m.getColumnOrAny(colName);
                if (col == null) {
                    LOG.warn(String.format("Column [%s] was not found ", colName));
                } else {
                    col.setBytes(instance, CellUtil.cloneValue(c), colName);
                }
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Unable to initialize the result for class %s ", m.getEntityType()));
        }
        return instance;
    }
}
