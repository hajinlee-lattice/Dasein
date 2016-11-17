package com.latticeengines.datafabric.entitymanager;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datafabric.RecordKey;

public interface BaseFabricEntityMgr<T> {

    void create(T entity);

    public void batchCreate(List<T> entities);

    void update(T entity);

    void delete(T entity);

    T findByKey(T entity);

    T findByKey(String id);

    Map<String, Object> findAttributesByKey(String id);

    List<T> batchFindByKey(List<String> ids);

    List<T> findByProperties(Map<String, String> properties);

    void publish(T entity);

    void publish(RecordKey recordKey, T entity);

    void addConsumer(String processorName, FabricEntityProcessor proc, int threadNumber);

    void removeConsumer(String processorName, int waitTime);

    boolean isDisabled();

    void init();

    String getRepository();

    String getRecordType();

}
