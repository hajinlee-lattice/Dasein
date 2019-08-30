package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.CatalogEntityMgr;
import com.latticeengines.apps.cdl.repository.jpa.writer.CatalogWriterRepository;
import com.latticeengines.apps.cdl.repository.reader.CatalogReaderRepository;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("catalogEntityMgr")
public class CatalogEntityMgrImpl extends JpaEntityMgrRepositoryImpl<Catalog, Long> implements CatalogEntityMgr {

    @Inject
    private CatalogReaderRepository readerRepository;

    @Inject
    private CatalogWriterRepository writerRepository;

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Catalog findByNameAndTenant(@NotNull String name, @NotNull Tenant tenant) {
        Preconditions.checkNotNull(name, "Name should not be null");
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        List<Catalog> catalogs = readerRepository.findByNameAndTenant(name, tenant);
        if (CollectionUtils.isEmpty(catalogs)) {
            return null;
        }
        Preconditions.checkArgument(catalogs.size() == 1, String.format(
                "Catalog %s should be unique for tenant %s, got %d instead", name, tenant.getId(), catalogs.size()));
        return catalogs.get(0);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Catalog> findByTenant(@NotNull Tenant tenant) {
        Preconditions.checkNotNull(tenant, "Tenant should not be null");
        return readerRepository.findByTenant(tenant);
    }

    @Override
    public BaseJpaRepository<Catalog, Long> getRepository() {
        return writerRepository;
    }
}
