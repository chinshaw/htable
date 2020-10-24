package com.ch.htable;

import com.ch.htable.core.HEntityManager;

public interface IHbaseRepository<E> {

    default void save(E entity) {
        getEntityManager().save(entity);
    }

    default void saveAll(Iterable<E> entities) {
        getEntityManager().saveAll(entities, getClassType());
    }

    HEntityManager getEntityManager();

    Class<E> getClassType();
}
