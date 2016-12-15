package com.latticeengines.datafabric.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.datafabric.TimeSeriesFabricEntity;

public interface TimeSeriesFabricEntityMgr<T extends TimeSeriesFabricEntity> extends CompositeFabricEntityMgr<T> {
    List<T> findTimeSeries(String parentName, String parentId, String entityName, String entityId, String bucket);
}
