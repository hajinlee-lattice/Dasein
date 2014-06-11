package com.latticeengines.dataplatform.dao;

import java.util.List;


public interface BaseDao<T> {

    //void load();
    
    //void save();
	void create(T entity);
	
	void createOrUpdate(T entity);
	
	void update(T entity);
	
	void delete(T entity);
	
	boolean containInSession(T entity);
	
    //void post(T entity);
    
    //void clear();	

    // T getById(String id);

    // List<T> getAll();
    
    List<T> findAll();
    
    T findByKey(T entity);
   
    T findByKey(Class<T> entityClz, Long key); 
    	    
    T deserialize(String id, String content);
    
    String serialize(T entity);
    
    //void deleteStoreFile();
        
}
