package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.dao.CatalogDao;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.repository.CatalogRepository;
import com.latticeengines.apps.cdl.repository.reader.CatalogReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.CatalogWriterRepository;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("catalogEntityMgr")
public class CatalogEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<CatalogRepository, Catalog, Long> implements
        CatalogEntityMgr {

    @Inject
    private CatalogEntityMgrImpl self;

    @Inject
    private CatalogDao catalogDao;

    @Inject
    private CatalogReaderRepository readerRepository;

    @Inject
    private CatalogWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Catalog> findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant) {
        Preconditions.checkNotNull(name, "Name should not be null");
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return getCurrentRepo().findByNameAndTenant(name, tenant);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Catalog> findByTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return getCurrentRepo().findByTenant(tenant);
    }

    @Override
    protected CatalogRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected CatalogRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<CatalogRepository, Catalog, Long> getSelf() {
        return self;
    }

    @Override
    public BaseDao<Catalog> getDao() {
        return catalogDao;
    }

    private CatalogRepository getCurrentRepo() {
        return isReaderConnection() ? readerRepository : writerRepository;
    }
}
