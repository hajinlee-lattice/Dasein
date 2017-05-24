package com.latticeengines.dantedb.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;

public interface BaseDanteDao<T> extends BaseDao<T> {

    T findByExternalID(String externalID);
    //
    // void create(T entity);
    //
    // void createOrUpdate(T entity);
    //
    // void update(T entity);
    //
    // void delete(T entity);
    //
    // void deleteAll();
    //
    // boolean containInSession(T entity);
    //
    // List<T> findAll();
    //
    // T findByKey(T entity);
    //
    // T findByKey(Class<T> entityClz, Long key);
    //
    // <F> T findByField(String fieldName, F value);
    //
    // <F> List<T> findAllByField(String fieldName, F value);
    //
    // T findByFields(String... fieldsAndValues);
    //
    // List<T> findAllByFields(String... fieldsAndValues);
}
