package com.latticeengines.dataplatform.entitymanager;

import com.latticeengines.dataplatform.dao.BaseDao;

public interface BaseEntityMgr<T> {

    BaseDao<T> getDao();
    
    void save();

    void load();
    
    void post(T entity);
    
    void clear();
    
    T getById(Object id);

}
