package com.latticeengines.dantedb.exposed.entitymgr.impl;

import java.util.List;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dantedb.exposed.entitymgr.BaseDanteEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.HasDanteAuditingFields;

public abstract class BaseDanteEntityMgrImpl<T extends HasDanteAuditingFields> extends BaseEntityMgrImpl<T>
        implements BaseDanteEntityMgr<T> {
	
	public BaseDanteEntityMgrImpl() {
    }

    public abstract BaseDao<T> getDao(); 

    @Transactional(value="dantedb", propagation = Propagation.REQUIRED)
    @Override
    public void create(T entity) {
        getDao().create(entity);
    }

    @Transactional(value="dantedb", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(T entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(value="dantedb", propagation = Propagation.REQUIRED)
    @Override
    public void update(T entity) {
        getDao().update(entity);
    }

    @Transactional(value="dantedb", propagation = Propagation.REQUIRED)
    @Override
    public void delete(T entity) {
        getDao().delete(entity);
    }
    
    @Transactional(value="dantedb", propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(value="dantedb", propagation = Propagation.REQUIRED, readOnly=true)
    @Override
    public boolean containInSession(T entity) {
        return getDao().containInSession(entity);
    }
    

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Transactional(value="dantedb", propagation = Propagation.REQUIRED, readOnly=true)
    @Override
    public T findByKey(T entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public T findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }
    
    @Transactional(value="dantedb", propagation = Propagation.REQUIRED, readOnly=true)
    @Override
    public List<T> findAll() {
        return getDao().findAll();
    }
}
