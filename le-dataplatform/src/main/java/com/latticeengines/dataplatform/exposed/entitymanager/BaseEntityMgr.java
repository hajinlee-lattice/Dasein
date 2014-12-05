package com.latticeengines.dataplatform.exposed.entitymanager;

import java.util.List;

import com.latticeengines.dataplatform.dao.BaseDao;

public interface BaseEntityMgr<T> {

    BaseDao<T> getDao();
    
    void create(T entity);

    void createOrUpdate(T entity);
     
    void update(T entity);
    
    void delete(T entity);

    void deleteAll();
    
    boolean containInSession(T entity);
    
    T findByKey(T entity);
 
    List<T> findAll();

    
}
