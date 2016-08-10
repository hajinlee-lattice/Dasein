package com.latticeengines.datafabric.entitymanager;

import java.util.List;
import java.util.Map;

public interface BaseFabricEntityMgr<T> {

    void create(T entity);

    public void batchCreate(List<T> entities);

    void update(T entity);

    void delete(T entity);

    T findByKey(T entity);

    T findByKey(String id);

    List<T> batchFindByKey(List<String> ids);

    List<T> findByProperties(Map<String, String> properties);

    void publish(T entity);

    void addConsumer(String processorName, FabricEntityProcessor proc, int threadNumber);

    void removeConsumer(String processorName, int waitTime);

    boolean isDisabled();

    public void init();

}


