package com.latticeengines.pls.entitymanager.impl;

import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class MultiTenantEntityMgrImpl<T extends HasPid> extends BaseEntityMgrImpl<T> {

}
