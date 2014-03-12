package com.latticeengines.dataplatform.dao;


public interface BaseDao<T> {

    void load();
    
    void save();
    
    void post(T entity);
    
    void clear();

    T getById(String id);
    
    T deserialize(String id, String content);
    
    String serialize(T entity);
    
    void deleteStoreFile();
}
