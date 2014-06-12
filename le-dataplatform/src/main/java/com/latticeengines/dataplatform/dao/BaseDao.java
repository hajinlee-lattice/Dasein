package com.latticeengines.dataplatform.dao;

import java.util.List;

public interface BaseDao<T> {

    void create(T entity);

    void createOrUpdate(T entity);

    void update(T entity);

    void delete(T entity);

    boolean containInSession(T entity);

    List<T> findAll();

    T findByKey(T entity);

    T findByKey(Class<T> entityClz, Long key);

}
