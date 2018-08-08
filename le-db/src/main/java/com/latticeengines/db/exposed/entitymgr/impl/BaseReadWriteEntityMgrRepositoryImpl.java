package com.latticeengines.db.exposed.entitymgr.impl;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class BaseReadWriteEntityMgrRepositoryImpl<T extends HasPid, ID> extends
        BaseEntityMgrRepositoryImpl<T, ID> implements BaseEntityMgrRepository<T, ID> {

    public BaseReadWriteEntityMgrRepositoryImpl() {
    }

    public abstract BaseJpaRepository<T, ID> getRepositoryByContext();

    @Override
    public BaseJpaRepository<T, ID> getRepository() {
        return getRepositoryByContext();
    }
}
