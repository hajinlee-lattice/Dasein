package com.latticeengines.db.exposed.dao;

import java.util.Collection;
import java.util.List;

public interface BaseDao<T> {

    void create(T entity);

    void createOrUpdate(T entity);

    void create(Collection<T> entities);

    void create(Collection<T> entities, boolean setAuditFields);

    void update(T entity);

    void delete(T entity);

    void deleteById(String id, boolean hardDelete);

    void deleteByPid(Long pid, boolean hardDelete);

    void deleteByName(String name, boolean hardDelete);

    void revertDeleteById(String id);

    void revertDeleteByPid(Long pid);

    void revertDeleteByName(String name);

    void deleteAll();

    boolean containInSession(T entity);

    List<T> findAll();

    T findByKey(T entity);

    T findByKey(Class<T> entityClz, Long key);

    <F> T findByField(String fieldName, F value);

    <F> List<T> findAllByField(String fieldName, F value);

    T findByFields(Object... fieldsAndValues);

    List<T> findAllByFields(Object... fieldsAndValues);

    List<T> findAllSortedByFieldWithPagination(int offset, int limit, String sortByField,
            Object... fieldsAndValues);

    T merge(T entity);
    
    long findAllCount();

    long findCountByFields(Object... fieldsAndValues);

    void flushSession();
    
    void clearSession();
    
    int getBatchSize();
}
