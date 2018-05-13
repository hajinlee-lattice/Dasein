package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataCollectionStatusDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataCollectionStatusRepositoy;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataCollectionStatusEntityMgr")
public class DataCollectionStatusEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataCollectionStatus, Long>
        implements DataCollectionStatusEntityMgr {

    @Inject
    private DataCollectionStatusRepositoy repository;

    @Inject
    private DataCollectionStatusDao dataCollectionDao;

    @Override
    public BaseJpaRepository<DataCollectionStatus, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<DataCollectionStatus> getDao() {
        return dataCollectionDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataCollectionStatus findByTenant(Tenant tenant) {
        return repository.findByTenant(tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void saveOrUpdateStatus(DataCollectionStatus status) {
        getDao().createOrUpdate(status);
    }




}
