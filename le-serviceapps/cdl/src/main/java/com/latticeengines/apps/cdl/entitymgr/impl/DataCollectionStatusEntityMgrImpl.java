package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataCollectionStatusDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionStatusEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataCollectionStatusRepositoy;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataCollectionStatusEntityMgr")
public class DataCollectionStatusEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataCollectionStatus, Long>
        implements DataCollectionStatusEntityMgr {

    @Inject
    private DataCollectionStatusRepositoy repository;

    @Inject
    private DataCollectionStatusDao dataCollectionStatusDao;

    @Override
    public BaseJpaRepository<DataCollectionStatus, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<DataCollectionStatus> getDao() {
        return dataCollectionStatusDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public DataCollectionStatus findByTenantAndVersion(Tenant tenant, DataCollection.Version version) {
        List<DataCollectionStatus> stats = repository.findByTenantAndVersion(tenant, version);
        if (CollectionUtils.isEmpty(stats)) {
            return null;
        }
        if (stats.size() == 1) {
            return stats.get(0);
        }
        throw new RuntimeException(
                "There are " + stats.size() + " data collection status in current tenant with the version " + version);
    }




}
