package com.latticeengines.datafabric.entitymanager;

import java.util.List;
import java.util.Map;

public interface BaseFabricEntityMgr<T> {

    void create(T entity);

    void update(T entity);

    void delete(T entity);

    T findByKey(T entity);

    List<T> findByProperties(Map<String, String> properties);

    void publish(T entity);

    void addConsumer(String processorName, FabricEntityProcessor proc, int threadNumber);

    void removeConsumer(String processorName, int waitTime);

    boolean isDisabled();

}


