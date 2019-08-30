package com.latticeengines.db.exposed.entitymgr.impl;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;

public abstract class JpaEntityMgrRepositoryImpl <T, ID> implements BaseEntityMgrRepository<T , ID> {

    public abstract BaseJpaRepository<T, ID> getRepository();

    @Override
    public BaseDao<T> getDao() {
        return null;
    }

    @Deprecated // use save
    @Override
    public void create(T entity) {
        save(entity);
    }

    @Deprecated // use save
    @Override
    public void update(T entity) {
        save(entity);
    }

    @Deprecated // use save
    @Override
    public void createOrUpdate(T entity) {
        save(entity);
    }

    public void save(T entity) {
        setAuditingFields(entity);
        getRepository().save(entity);
    }

    @Override
    public void delete(T entity) {
        getRepository().delete(entity);
    }

    @Override
    public void deleteAll() {
        getRepository().deleteAll();
    }

    @Override
    public List<T> findAll() {
        return getRepository().findAll();
    }

    @Override
    public boolean containInSession(T entity) {
        throw new UnsupportedOperationException("containInSession not implemented in JPA entity mgr yet.");
    }

    @Override
    public T findByKey(T entity) {
        throw new UnsupportedOperationException("findByKey not implemented in JPA entity mgr yet, because we don't want to enforce Long PID.");
    }

    @Override
    public T findByField(String fieldName, Object value) {
        throw new UnsupportedOperationException("findByField not implemented in JPA entity mgr yet.");
    }
    
    @Override
    public ParsedDependencies parse(T entity, T existingEntity) {
        return null;
    }

    private void setAuditingFields(T entity) {
        if (entity instanceof HasAuditingFields) {
            Date now = new Date();
            if (entity instanceof HasPid && ((HasPid) entity).getPid() == null) {
                ((HasAuditingFields) entity).setCreated(now);
            }
            ((HasAuditingFields) entity).setUpdated(now);
        }
    }
}
