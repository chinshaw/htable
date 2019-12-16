package com.ch.htable;

import com.ch.htable.ValueAccessor.MethodAccessor;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * This is loosely based on ideas from {@link javax.persistence.metamodel.Metamodel}. This is a singleton that contains
 * all things known about hbase supported mapped entities. The foundational base is that we will map pojo classes annotated
 * with {@link HTable}, {@link HColumn}, and {@link HId} in order to convert entities to column families and back.
 * This class assists in analyzing such entities and creating the required meta data to facilitate these tasks. Once
 * a class has been analyzed it will be cached for future lookups. This has safeguards to keep you from analyzing a class
 * multiple times due to the performance hit of reflection methods.
 */
class AnnotationAnalyzer {

    /**
     * The simple underlying map of class to entity model objects.
     */
    private static final ConcurrentHashMap<Class<?>, EntityClassModel> meta = new ConcurrentHashMap<>();
    private static AnnotationAnalyzer instance = null;

    private AnnotationAnalyzer() {}

    public static AnnotationAnalyzer getInstance() {
        if (instance == null) {
            synchronized (AnnotationAnalyzer.class) {
                instance = new AnnotationAnalyzer();
            }
        }
        return instance;
    }

    /**
     * This will retrieve an entity model. If the entity model is not in the cache it will be analyzed first.
     * @param clazz The class to lookup.
     * @param <T> The type of the class to lookup.
     * @return The {@link EntityClassModel} instance for the class.
     */
    <T> EntityClassModel<T> entityModel(Class<T> clazz) {
        if (! meta.containsKey(clazz)) {
            registerType(clazz);
        }
        return meta.get(clazz);
    }

    <T> EntityClassModel<T> registerType(Class<T> clazz) {
        if (! meta.containsKey(clazz)) {
            final EntityClassModel<T> em = analyzeClass(clazz);
            meta.put(clazz, em);
            analyzeMethods(em);

            // Check for the id annotation
            checkState(em.getIdentifier() != null, "Class %s is missing HId annotation", clazz);
            return em;
        } else {
            return meta.get(clazz);
        }
    }

    private <T> EntityClassModel<T> analyzeClass(Class<T> c) {
        if (! c.isAnnotationPresent(HTable.class)) {
            throw new RuntimeException(String.format("Attempt to register class %s that does not have table option", c));
        }

        HTable t = c.getAnnotation(HTable.class);
        final String tableName = t != null ? t.name() : c.getName().toLowerCase();
        return new EntityClassModel<>(c, tableName, t.cf());
    }

    private void analyzeMethods(EntityClassModel em) {
        final Class<?> clazz = em.getEntityType();
        for (Method m : clazz.getDeclaredMethods()) {
            analyzeMethod(em, m);
        }
    }

    /**
     * This is really exposed only for testing purposes.
     * @param clazz The java class to add.
     * @param meta The meta model to add.
     */
    void put(Class<?> clazz, EntityClassModel meta) {
        AnnotationAnalyzer.meta.put(clazz, meta);
    }


    /**
     * Analyze a single method and figure out if it is annotated with
     * either {@link HColumn} or {@link HId}. If either of these
     * annotations exist then the corresponding method will be invoked
     * to handle building a {@link ColumnModel} or an {@link EntityClassModel.Identifier}
     * @param em
     * @param m
     */
    private void analyzeMethod(EntityClassModel em, Method m) {
        if (m.isAnnotationPresent(HColumn.class)) {
            final HColumn a = m.getAnnotation(HColumn.class);
            final ColumnMeta<?> column =  em.hasColumn(a.name()) ? em.getColumnOrAny(a.name()) : new ColumnMeta<>(a.name());
            em.addColumn(a.name(), columnMethod(column, m, a));
        }

        if (m.isAnnotationPresent(HId.class)) {
            HId a = m.getAnnotation(HId.class);
            em.setIdentifier(idMethod(m, a));
        }

        if (m.isAnnotationPresent(HAnyColumn.class)) {
            final AnyColumnMeta<?> column =  em.hasAnyColumn() ?
                    (AnyColumnMeta<?>) em.getAnyColumn() : new AnyColumnMeta<>("ANY_COLUMN");
            em.setAnyColumn(anyColumnMethod(column,m, m.getAnnotation(HAnyColumn.class)));
        }
    }

    /**
     * Analyze a method that is annotated with {@link HColumn}.
     * @param <T> The value type for the column.
     * @param column The column model for the value.
     * @param m The method that will be invoked to get teh value.
     * @param a The {@link HColumn} annotation.
     * @return The column definition of type.
     */
    private <T> ColumnMeta<T> columnMethod(ColumnMeta<T> column, Method m, HColumn a) {
        try {
            column.setConverter(a.converter().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Failed to analyze method {} ", m.getName()), e);
        }

        final String methodName = m.getName();

        if (column.getValueAccessor() == null) {
            column.setValueAccessor(new MethodAccessor<>());
        }

        if (methodName.startsWith("is") || methodName.startsWith("get")) {
            ((MethodAccessor)column.getValueAccessor()).setGetter(m);
        }

        if (methodName.startsWith("set")) {
            ((MethodAccessor)column.getValueAccessor()).setSetter(m);
        }
        return column;
    }

    /**
     * Analyze a method that is annotated with {@link HAnyColumn}. This is a wild card annotation since not all
     * columns can be mapped naturally.
     * @param <T> The value type for the column.
     * @param column The column model for the value.
     * @param m The method that will be invoked to get teh value.
     * @param a The {@link HColumn} annotation.
     * @return The column definition of type.
     */
    private <T> ColumnMeta<T> anyColumnMethod(ColumnMeta<?> column, Method m, HAnyColumn a) {
        try {
            column.setConverter(a.converter().newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Failed to analyze method %s ", m.getName()), e);
        }

        final String methodName = m.getName();

        if (column.getValueAccessor() == null) {
            column.setValueAccessor(new MethodAccessor<>());
        }

        if (methodName.startsWith("is") || methodName.startsWith("get")) {
            ((MethodAccessor)column.getValueAccessor()).setGetter(m);
        }

        if (methodName.startsWith("set")) {
            ((MethodAccessor)column.getValueAccessor()).setSetter(m);
        }
        return (ColumnMeta<T>) column;
    }

    /**
     * Analyze an Hid field and lookup the id method and converter implementation.
     * @param m The method used to retrieve the value of the id
     * @param a The annotation instance.
     * @return The identifier handler for method m.
     */
    private EntityClassModel.Identifier idMethod(Method m, HId a) {
        final ColumnConverter converter;
        try {
            converter = a.converter().newInstance();
            final ValueAccessor ma = new MethodAccessor();
            ((MethodAccessor) ma).setGetter(m);
            return new EntityClassModel.Identifier(converter, ma);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Failed to analyze method %s ", m.getName()), e);
        }
    }
}
