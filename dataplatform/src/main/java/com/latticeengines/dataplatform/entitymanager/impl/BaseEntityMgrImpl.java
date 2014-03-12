package com.latticeengines.dataplatform.entitymanager.impl;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.HasId;

public abstract class BaseEntityMgrImpl<T extends HasId<?>> implements BaseEntityMgr<T> {

    public BaseEntityMgrImpl() {
    }
    
    public abstract BaseDao<T> getDao();
    
    protected void deleteStoreFile() {
        getDao().deleteStoreFile();
    }
    
    @Override
    public void save() {
        getDao().save();
    }

    @Override
    public void load() {
        getDao().load();
    }
    
    @Override
    public void post(T entity) {
        getDao().post(entity);
    }
    
    @Override
    public void clear() {
        getDao().clear();
    }
    
    @Override
    public T getById(Object id) {
        return (T) getDao().getById(id.toString());
    }
}
