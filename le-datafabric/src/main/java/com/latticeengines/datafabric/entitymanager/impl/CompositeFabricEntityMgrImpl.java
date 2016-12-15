package com.latticeengines.datafabric.entitymanager.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.datafabric.entitymanager.CompositeFabricEntityMgr;
import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;

public class CompositeFabricEntityMgrImpl<T extends CompositeFabricEntity> extends BaseFabricEntityMgrImpl<T>
                                         implements CompositeFabricEntityMgr<T> {

    public CompositeFabricEntityMgrImpl(Builder builder) {
        super(builder);
    }

    public List<T> findChildren(String parentId, String entityName) {
        String parentKey = parentId + "_" + entityName;
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(tableIndex.getHashKeyField(), parentKey);
        return findByProperties(properties);
    }

    public void deleteChildren(String parentId, String entityName) {
        return;
    }

    public void deleteByKey(String id) {
        return;
    }
}
