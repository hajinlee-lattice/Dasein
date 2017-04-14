package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.DependableObject;

public interface DependableObjectEntityMgr extends BaseEntityMgr<DependableObject> {
    DependableObject find(String type, String name);

    void create(DependableObject dependableObject);

    void delete(String type, String name);
}