package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.DataCollectionArtifactDao;
import com.latticeengines.apps.cdl.entitymgr.DataCollectionArtifactEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.DataCollectionArtifactRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("dataCollectionArtifactEntityMgr")
public class DataCollectionArtifactEntityMgrImpl extends BaseEntityMgrRepositoryImpl<DataCollectionArtifact, Long>
    implements DataCollectionArtifactEntityMgr {

    @Inject
    private DataCollectionArtifactRepository repository;

    @Inject
    private DataCollectionArtifactDao dataCollectionArtifactDao;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataCollectionArtifact> findByTenantAndVersion(Tenant tenant, DataCollection.Version version) {
        return repository.findByTenantAndVersionOrderByCreateTimeDesc(tenant, version);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataCollectionArtifact> findByTenantAndStatusAndVersion(Tenant tenant,
                                                                        DataCollectionArtifact.Status status,
                                                                        DataCollection.Version version) {
        return repository.findByTenantAndStatusAndVersionOrderByCreateTimeDesc(tenant, status, version);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<DataCollectionArtifact> findByTenantAndNameAndVersion(Tenant tenant, String name,
                                                                      DataCollection.Version version) {
        return repository.findByTenantAndNameAndVersionOrderByCreateTimeDesc(tenant, name, version);
    }

    @Override
    public BaseJpaRepository<DataCollectionArtifact, Long> getRepository() {
        return repository;
    }

    @Override
    public BaseDao<DataCollectionArtifact> getDao() {
        return dataCollectionArtifactDao;
    }
}
