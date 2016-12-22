package com.latticeengines.db.exposed.entitymgr.impl;

import java.util.List;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class BaseEntityMgrImpl<T extends HasPid> implements BaseEntityMgr<T> {

    public BaseEntityMgrImpl() {
    }

    public abstract BaseDao<T> getDao();

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void create(T entity) {
        getDao().create(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(T entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void update(T entity) {
        getDao().update(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void delete(T entity) {
        getDao().delete(entity);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public boolean containInSession(T entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByKey(T entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<T> findAll() {
        return getDao().findAll();
    }

}
