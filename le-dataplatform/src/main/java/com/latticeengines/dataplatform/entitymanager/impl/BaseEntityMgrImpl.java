package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.entitymanager.BaseEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public abstract class BaseEntityMgrImpl<T extends HasId<?>> implements BaseEntityMgr<T> {

    public BaseEntityMgrImpl() {
    }
    
    public abstract BaseDao<T> getDao();
    
    public void deleteStoreFile() {
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
    
    @Override
    public List<T> getAll() {
        return getDao().getAll();
    }
}
