package com.latticeengines.datafabric.entitymanager.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.datafabric.entitymanager.TimeSeriesFabricEntityMgr;
import com.latticeengines.domain.exposed.datafabric.TimeSeriesFabricEntity;

public class TimeSeriesFabricEntityMgrImpl<T extends TimeSeriesFabricEntity>
                                          extends CompositeFabricEntityMgrImpl<T>
                                          implements TimeSeriesFabricEntityMgr<T> {

    public TimeSeriesFabricEntityMgrImpl(Builder builder) {
        super(builder);
    }

    public List<T> findTimeSeries(String parentName, String parentId, String entityName, String entityId, String bucket) {
        String parentKey = parentName + "_" + parentId + "_" + entityName;
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(tableIndex.getHashKeyField(), parentKey);
        properties.put(tableIndex.getRangeKeyField(), entityId);
        properties.put(tableIndex.getBucketKeyField(), bucket);
        return findByProperties(properties);
    }
}
