package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class BaseOrchestrationEntityMgrImpl<T extends HasPid> extends BaseEntityMgrImpl<T> {

    public BaseOrchestrationEntityMgrImpl() {
    }

    public abstract BaseDao<T> getDao();

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public void create(T entity) {
        getDao().create(entity);
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(T entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public void update(T entity) {
        getDao().update(entity);
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public void delete(T entity) {
        getDao().delete(entity);
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public boolean containInSession(T entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public T findByKey(T entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }

    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    @Override
    public List<T> findAll() {
        return getDao().findAll();
    }

}
