package com.latticeengines.datafabric.entitymanager;

import java.util.List;
import java.util.Map;

public interface BaseFabricEntityMgr<T> {

    void create(T entity);

    void batchCreate(List<T> entities);

    void update(T entity);

    void delete(T entity);

    T findByKey(T entity);

    T findByKey(String id);

    Map<String, Object> findAttributesByKey(String id);

    List<T> batchFindByKey(List<String> ids);

    List<T> findByProperties(Map<String, String> properties);

    boolean isDisabled();

    void init();

    String getRepository();

    String getRecordType();

}
