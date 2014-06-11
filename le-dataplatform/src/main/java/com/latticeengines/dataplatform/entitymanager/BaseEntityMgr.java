package com.latticeengines.dataplatform.entitymanager;

import java.util.List;

import com.latticeengines.dataplatform.dao.BaseDao;

public interface BaseEntityMgr<T> {

    BaseDao<T> getDao();
    
    void create(T entity);

    void createOrUpdate(T entity);
     
    void update(T entity);
    
    void delete(T entity);

    boolean containInSession(T entity);
    
    T findByKey(T entity);
 
    List<T> findAll();
    

/*    void save();

    void load();
    
    void post(T entity);
    
    void clear();*/
   /* 
    T getById(Object id);

    List<T> getAll();
    
    T getByKey(T entity);
*/
}
