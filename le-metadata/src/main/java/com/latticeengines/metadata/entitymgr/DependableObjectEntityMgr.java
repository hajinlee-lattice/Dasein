package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.domain.exposed.metadata.DependableType;

public interface DependableObjectEntityMgr extends BaseEntityMgr<DependableObject> {
    DependableObject find(DependableType type, String name);

    void create(DependableObject dependableObject);

    void delete(DependableType type, String name);
}