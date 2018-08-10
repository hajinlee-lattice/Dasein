package com.latticeengines.db.exposed.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;

public interface BaseEntityMgr<T> {

    BaseDao<T> getDao();

    void create(T entity);

    void createOrUpdate(T entity);

    void update(T entity);

    void delete(T entity);

    void deleteAll();

    boolean containInSession(T entity);

    T findByKey(T entity);

    T findByField(String fieldName, Object value);

    List<T> findAll();

    ParsedDependencies parse(T entity, T existingEntity);

}
