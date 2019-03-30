package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataCollectionStatusHistoryDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusHistoryEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataCollectionStatusHistoryRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusHistory;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataCollectionStatusHistoryEntityMgr")
public class DataCollectionStatusHistoryEntityMgrImpl
        extends BaseEntityMgrRepositoryImpl<DataCollectionStatusHistory, Long>
        implements DataCollectionStatusHistoryEntityMgr {

    @Inject
    private DataCollectionStatusHistoryDao dataCollectionStatusHistoryDao;

    @Inject
    private DataCollectionStatusHistoryRepository dataCollectionStatusRepository;


    @Override
    public BaseJpaRepository<DataCollectionStatusHistory, Long> getRepository() {
        return dataCollectionStatusRepository;
    }

    @Override
    public BaseDao<DataCollectionStatusHistory> getDao() {
        return dataCollectionStatusHistoryDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataCollectionStatusHistory> findByTenantOrderByCreationTimeDesc(Tenant tenant) {
        return dataCollectionStatusRepository.findByTenantOrderByCreationTimeDesc(tenant);
    }

}
