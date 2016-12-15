package com.latticeengines.datafabric.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.datafabric.CompositeFabricEntity;

public interface CompositeFabricEntityMgr<T extends CompositeFabricEntity> extends BaseFabricEntityMgr<T>{
    List<T> findChildren(String parentId, String entityName);
    void deleteChildren(String parentId, String entityName);
    void deleteByKey(String id);
}
