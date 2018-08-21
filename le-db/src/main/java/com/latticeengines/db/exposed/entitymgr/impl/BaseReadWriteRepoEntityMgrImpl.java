package com.latticeengines.db.exposed.entitymgr.impl;

import java.util.List;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.db.exposed.util.DBConnectionContext;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

public abstract class BaseReadWriteRepoEntityMgrImpl<R extends BaseJpaRepository<T, ID>, T extends HasPid, ID> extends
        BaseEntityMgrRepositoryImpl<T, ID> implements BaseEntityMgrRepository<T, ID> {

    public BaseReadWriteRepoEntityMgrImpl() {
    }

    protected abstract R getReaderRepo();
    protected abstract R getWriterRepo();
    protected abstract BaseReadWriteRepoEntityMgrImpl<R, T, ID> getSelf();

    @Override
    public BaseJpaRepository<T, ID> getRepository() {
        return getWriterRepo();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<T> findAll() {
        if (Boolean.TRUE.equals(DBConnectionContext.isReaderConnection())) {
            return getSelf().findAllFromReader();
        } else {
            return getWriterRepo().findAll();
        }
    }

    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<T> findAllFromReader() {
        return getReaderRepo().findAll();
    }

    protected boolean isReaderConnection() {
        return Boolean.TRUE.equals(DBConnectionContext.isReaderConnection());
    }

}
