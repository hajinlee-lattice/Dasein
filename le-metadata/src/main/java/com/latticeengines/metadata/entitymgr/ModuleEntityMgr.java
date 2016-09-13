package com.latticeengines.metadata.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Module;

public interface ModuleEntityMgr extends BaseEntityMgr<Module> {

    Module findByName(String name);

}
