package com.latticeengines.db.exposed.dao;

import java.util.List;

public interface BaseDao<T> {

    void create(T entity);

    void createOrUpdate(T entity);

    void update(T entity);

    void delete(T entity);

    void deleteAll();

    boolean containInSession(T entity);

    List<T> findAll();

    T findByKey(T entity);

    T findByKey(Class<T> entityClz, Long key);

    <F> T findByField(String fieldName, F value);

    <F> List<T> findAllByField(String fieldName, F value);

    T findByFields(Object... fieldsAndValues);

    List<T> findAllByFields(Object... fieldsAndValues);

    T merge(T entity);
}
