package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
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
    public boolean containInSession(T entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Override
    public T findByKey(T entity) {
        return getDao().findByKey(entity);
    }

    @Override
    public List<T> findAll() {
        return getDao().findAll();
    }

}
